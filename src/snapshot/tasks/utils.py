import json
import logging
import re
import shutil
import typing
from pathlib import Path

import nibabel as nb
import polars as pl

from snapshot import datasets

# values to parse from src files as null (n/a will be used for output)
NULLS = ["", "na", "n/a"]

# https://bids-specification.readthedocs.io/en/v1.9.0/common-principles.html#units
DATETIME_FORMAT = "%Y-%m-%d%H:%M:%S"

SIDECAR_FIELDS_TO_REMOVE = ["InstitutionAddress"]


def to_bids_tsv(d: pl.DataFrame, dst: Path) -> None:
    if dst.exists():
        dst.unlink()
    d.write_csv(dst, separator="\t", null_value="n/a", datetime_format=DATETIME_FORMAT)


def _get_entity(f: Path, pattern: str) -> str:
    possibility = re.findall(pattern, str(f))
    if not len(possibility):
        raise ValueError
    return possibility[0]


def _get_sub(f: Path) -> str:
    return _get_entity(f=f, pattern=r"(?<=sub-)\d{5}")


def _get_ses(f: Path) -> str:
    return _get_entity(f=f, pattern=r"(?<=ses-)V[13]")


def write_participants(records: typing.Collection[int], outdir: Path) -> None:
    found_participants = [int(_get_sub(d)) for d in outdir.glob("sub*") if d.is_dir()]
    demographics = (
        pl.scan_csv(datasets.get_demographics(), null_values=NULLS)
        .select("record_id", "guid")
        .rename({"record_id": "sub"})
    )
    tbl = (
        pl.scan_csv(datasets.get_ilog(), null_values=NULLS)
        .select(sub="subject_id", ses="visit")
        .filter(pl.col("ses").str.contains("V1"))
        .filter(pl.col("sub").is_in(records))
        .filter(pl.col("sub").is_in(found_participants))
        .join(demographics, on="sub", how="left")
        .with_columns(participant_id=pl.concat_str(pl.lit("sub-"), pl.col("sub")))
        .drop("sub")
        .select("participant_id", "guid")
        .collect()
    )
    to_bids_tsv(tbl, dst=outdir / "participants.tsv")

    shutil.copy2(datasets.get_participants_json(), outdir)


def write_sessions(outdir: Path) -> None:
    mappings = {
        "visit": "session_id",
        "subject_id": "sub",
        "Face Mask": "face_mask",
        "acquisition_week": "acquisition_week",
        "fMRI T1 Tech Rating": "t1_tech_rating",
        "Cuff1 QST Pressure": "cuff1_pressure_from_qst",
        "Cuff1 Recalibrated Pressure": "cuff1_recalibrated_pressure",
        "Cuff1 Applied Pressure": "cuff1_applied_pressure",
        "Surgical site pain rest": "surgical_site_pain_rest",
        "Body pain rest": "body_pain_rest",
        "Surgical site pain after first scan": "surgical_site_pain_after_first_scan",
        "Body pain after first scan": "body_pain_after_first_scan",
        "Cuff pain after first scan": "cuff_pain_after_first_scan",
        "Cuff pain cuff1 beginning": "cuff_pain_cuff1_beginning",
        "Cuff pain cuff1 middle": "cuff_pain_cuff1_middle",
        "Cuff pain cuff1 end": "cuff_pain_cuff1_end",
        "Cuff pain cuff2 beginning": "cuff_pain_cuff2_beginning",
        "Cuff pain cuff2 middle": "cuff_pain_cuff2_middle",
        "Cuff pain cuff2 end": "cuff_pain_cuff2_end",
        "Cuff pain rest beginning": "cuff_pain_rest_beginning",
        "Cuff pain rest middle": "cuff_pain_rest_middle",
        "Cuff pain rest end": "cuff_pain_rest_end",
        "Surgical site pain after last scan": "surgical_site_pain_after_last_scan",
        "Body pain after last scan": "body_pain_after_last_scan",
        "Cuff contraindicated": "cuff_contraindicated",
        "Surgery Week": "surgery_week",
        "Cuff Leg": "cuff_leg",
    }
    ilog = (
        pl.read_csv(datasets.get_ilog(), null_values=NULLS)
        .filter(pl.col("visit").str.contains("V1"))
        .rename(mappings)
        .select(mappings.values())
        .join(
            datasets.get_device_serial_number_tbl(),
            how="left",
            on=["sub", "session_id"],
        )
        .with_columns(
            session_id=pl.col("session_id").str.replace("V", "ses-V"),
            face_mask=pl.col("face_mask").cast(bool),
            cuff_contraindicated=pl.col("cuff_contraindicated").cast(bool),
            acquisition_week=pl.col("acquisition_week").str.to_datetime("%Y-%m-%d"),
            surgery_week=pl.col("surgery_week").str.to_datetime("%Y-%m-%d"),
            protocol_name=pl.when(pl.col("session_id") == "ses-V1")
            .then(pl.lit("baseline_visit"))
            .otherwise(pl.lit("3mo_postop")),
        )
    )

    # look at parents of ses* dir rather than simply sub* because there may
    # be files that match sub* at the top level
    for sesdir in outdir.glob("sub*/ses*"):
        sub = int(_get_sub(sesdir))
        session = ilog.filter(pl.col("sub") == sub).drop("sub")
        to_bids_tsv(session, dst=sesdir.parent / f"sub-{sub}_sessions.tsv")

    shutil.copy2(datasets.get_sessions_json(), outdir / "sessions.json")


def update_scans(outdir: Path) -> None:
    qclog = pl.read_csv(datasets.get_qclog(), null_values=NULLS).select(
        "sub", "ses", "scan", "rating"
    )
    for scanstsv in outdir.rglob("*sub*scans.tsv"):
        scans = (
            pl.read_csv(scanstsv, null_values=NULLS, separator="\t")
            .select("filename")
            .with_columns(
                sub=pl.col("filename").str.extract(r"\d{5}", 0).cast(pl.Int64),
                ses=pl.col("filename").str.extract("V1|V3", 0),
                scan=pl.col("filename")
                .str.extract(
                    "(fmap|anat|dwi|cuff_run-01|cuff_run-02|rest_run-01|rest_run-02)",
                )
                .replace(
                    {
                        "anat": "T1w",
                        "dwi": "DWI",
                        "cuff_run-01": "CUFF1",
                        "cuff_run-02": "CUFF2",
                        "rest_run-01": "REST1",
                        "rest_run-02": "REST2",
                    }
                ),
            )
            .join(qclog, ("sub", "ses", "scan"), how="left")
            .select("filename", "rating")
        )
        scanstsv.unlink()
        to_bids_tsv(scans, scanstsv)
        if (scansjson := scanstsv.with_suffix(".json")).exists():
            scansjson.unlink()
    shutil.copy2(datasets.get_scans_json(), outdir / "scans.json")


def write_events(outdir: Path) -> None:
    pressure = pl.read_csv(datasets.get_applied_pressures(), null_values=NULLS)
    for nii in outdir.rglob("*bold.nii.gz"):
        fname = nii.with_name(nii.name.replace("bold.nii.gz", "events.tsv"))
        if "cuff_run-01" in nii.name:
            scan = "CUFF1"
        elif "cuff_run-02" in nii.name:
            scan = "CUFF2"
        elif "rest" in nii.name:
            if fname.exists():
                fname.unlink()
            continue
        else:
            scan = ""
        events = (
            pressure.filter(pl.col("record_id") == int(_get_sub(nii)))
            .filter(pl.col("visit") == _get_ses(nii))
            .filter(pl.col("scan") == scan)
            .with_columns(onset=0, duration=nb.nifti1.Nifti1Image.load(nii).shape[-1])
            .select("onset", "duration", "applied_pressure")
        )
        to_bids_tsv(events, fname)
    shutil.copy2(datasets.get_events_json(), outdir)


def write_readme(outdir: Path) -> None:
    shutil.copy2(datasets.get_readme(), outdir / "README")


def write_changes(outdir: Path) -> None:
    shutil.copy2(datasets.get_changes(), outdir / "CHANGES")


def write_cat12_tables_and_jsons(
    inroot: Path, outroot: Path, records: typing.Collection[int]
) -> None:
    dst = outroot / "derivatives" / "cat12"
    df = (
        pl.read_csv(
            inroot / "cat12" / "cluster_volumes.tsv", separator="\t", null_values=NULLS
        )
        .filter(pl.col("ses").str.contains("V1"))
        .filter(pl.col("sub").is_in(records))
    )
    to_bids_tsv(df, dst / "cluster_volumes.tsv")
    shutil.copy2(datasets.get_cat12_json(), dst / "cluster_volumes.json")


def write_freesurfer_tables_and_jsons(
    outroot: Path, inroot: Path, records: typing.Collection[int]
) -> None:
    dst = outroot / "derivatives" / "freesurfer"
    for tbl in ["aparc", "aseg", "headers", "gm_morph"]:
        df = (
            pl.read_csv(
                inroot / "freesurfer" / f"{tbl}.tsv", null_values=NULLS, separator="\t"
            )
            .filter(pl.col("ses").str.contains("V1"))
            .filter(pl.col("sub").is_in(records))
        )
        to_bids_tsv(df, dst / f"{tbl}.tsv")

    shutil.copy2(datasets.get_aparc_json(), dst / "aparc.json")
    shutil.copy2(datasets.get_aseg_json(), dst / "aseg.json")
    shutil.copy2(datasets.get_headers_json(), dst / "headers.json")
    shutil.copy2(datasets.get_gm_morph_json(), dst / "gm_morph.json")


def write_fslanat_tables_and_jsons(
    inroot: Path, outroot: Path, records: typing.Collection[int]
) -> None:
    dst = outroot / "derivatives" / "fslanat"
    df = (
        pl.read_csv(
            inroot / "fslanat" / "fslanat.tsv", null_values=NULLS, separator="\t"
        )
        .filter(pl.col("ses").str.contains("V1"))
        .filter(pl.col("sub").is_in(records))
    )
    to_bids_tsv(df, dst / "fslanat.tsv")

    shutil.copy2(datasets.get_fslanat_json(), dst / "fslanat.json")


def overwrite_tables(
    outjob: Path, records: typing.Collection[int], srcs: typing.Collection[str]
) -> None:
    for src in srcs:
        file = outjob / src
        df = (
            pl.read_csv(file, null_values=NULLS, separator="\t")
            .filter(pl.col("ses").str.contains("V1"))
            .filter(pl.col("sub").is_in(records))
        )
        to_bids_tsv(df, file)


def write_fcn_jsons(outroot: Path) -> None:
    dst = outroot / "derivatives" / "fcn"
    shutil.copy2(datasets.get_confounds_json(), dst / "confounds.json")
    shutil.copy2(datasets.get_connectivity_json(), dst / "connectivity.json")
    shutil.copy2(datasets.get_timeseries_json(), dst / "timeseries.json")
    shutil.copy2(datasets.get_disruption_json(), dst / "disruption.json")

    # handle typo in ledoit_wolf estimator
    for sub in (dst / "connectivity").glob("sub*"):
        for ses in sub.glob("ses*"):
            for task in ses.glob("task*"):
                for run in task.glob("run*"):
                    for atlas in run.glob("atlas*"):
                        for estimator in atlas.glob("estimator*"):
                            if "leodit_wolf" in estimator.name:
                                estimator.rename(
                                    estimator.with_name("estimator=ledoit_wolf")
                                )


def write_signatures_jsons(outroot: Path) -> None:
    dst = outroot / "derivatives" / "signatures"
    shutil.copy2(
        datasets.get_signatures_by_part_json(), dst / "signatures-by-part.json"
    )
    shutil.copy2(datasets.get_signatures_by_run_json(), dst / "signatures-by-run.json")
    shutil.copy2(datasets.get_signatures_by_tr_json(), dst / "signatures-by-tr.json")
    shutil.copy2(datasets.get_confounds_json(), dst / "confounds.json")
    shutil.copy2(
        datasets.get_signatures_by_part_diff_json(),
        dst / "signatures-by-part-diff.json",
    )
    shutil.copy2(
        datasets.get_signatures_by_run_diff_json(), dst / "signatures-by-run-diff.json"
    )
    shutil.copy2(
        datasets.get_signatures_by_tr_diff_json(), dst / "signatures-by-tr-diff.json"
    )


def clean_sidecars(root: Path) -> None:
    for sidecar in root.rglob("*json"):
        data: dict[str, typing.Any] = json.loads(sidecar.read_text())
        for field in SIDECAR_FIELDS_TO_REMOVE:
            if data.get(field):
                del data[field]
        sidecar.unlink()
        sidecar.write_text(json.dumps(data, indent=2, sort_keys=True))


def write_release_notes(outroot: Path) -> None:
    release_notes = datasets.get_release_notes()
    shutil.copyfile(release_notes, outroot / release_notes.name)


def write_postdtifit_jsons(outroot: Path) -> None:
    shutil.copy2(
        datasets.get_diffusion_regional_stats_json(),
        outroot / "derivatives" / "postdtifit" / "diffusion_regional_stats.json",
    )


def write_postgift_jsons(outroot: Path) -> None:
    dst = outroot / "derivatives" / "postgift"
    shutil.copy2(datasets.get_gift_amplitude_json(), dst / "amplitude.json")
    shutil.copy2(datasets.get_gift_biomarkers_json(), dst / "biomarkers.json")
    shutil.copy2(datasets.get_gift_connectivity_json(), dst / "connectivity.json")


def write_idps(inroot: Path, outroot: Path) -> None:
    records = datasets.get_recordids()
    dst = outroot / "idp"
    mask_volumes_json: dict[str, typing.Any] = json.loads(
        datasets.get_mask_volumes_json().read_text()
    )
    mris_json: dict[str, typing.Any] = json.loads(datasets.get_mri_json().read_text())

    columns = {
        "mask_volumes.tsv": list(mask_volumes_json.keys()),
        "mri.tsv": list(mris_json.keys()),
    }

    for f in ["mri.tsv", "mask_volumes.tsv"]:
        pl.scan_csv(inroot / "idp" / f, separator="\t").filter(
            pl.col("ses") == "V1", pl.col("sub").is_in(records)
        ).select(*columns[f]).sink_csv(dst / f, separator="\t", mkdir=True)

    shutil.copy2(datasets.get_mask_volumes_json(), dst / "mask_volumes.json")
    shutil.copy2(datasets.get_mri_json(), dst / "mri.json")


def write_dwi_biomarker1_jsons(outroot: Path) -> None:
    shutil.copy2(
        datasets.get_dwi_networks_json(),
        outroot
        / "derivatives"
        / "dwi_biomarker1"
        / "networks"
        / "network_summaries.json",
    )


def clean_fmriprep_logs(inroot: Path, outroot: Path) -> None:
    for toml_link in inroot.glob("sub*/log/*/fmriprep.toml"):
        # these should be symlinks, so resolve to determine whether they are V1 or V3
        src = toml_link.resolve()
        if "V3" in str(src):
            uuid = toml_link.parent.name
            sub = _get_sub(src)
            if (to_delete := (outroot / f"sub-{sub}" / "log" / uuid)).exists():
                logging.info(f"Removing V3 fmriprep log directory {to_delete}")
                shutil.rmtree(to_delete)


def remove_qsirecon_fsl_dtifit_v3_only(root: Path) -> None:
    derivatives = root / "qsirecon-fsl" / "derivatives" / "qsirecon-FSL"
    todelete = []
    for subdir in derivatives.glob("sub*"):
        if subdir.is_dir() and (len([subdir.iterdir()]) == 0):
            todelete.append(subdir.name)
            subdir.rmdir()

    for d in todelete:
        shutil.rmtree(root / "qsirecon-fsl" / d)

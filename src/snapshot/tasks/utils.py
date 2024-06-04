import json
import logging
import re
import shutil
import subprocess
import typing
from pathlib import Path

import datalad.api as dla
import ibis
import ibis.expr
import ibis.expr.types
import nibabel as nb
import pandas as pd
from ibis import _

from snapshot import datasets

GITATTRIBUTES = """
* annex.backend=MD5E
* annex.largefiles=(((mimeencoding=binary)and(largerthan=0))or((include=*gii)or(include=*svg)or(include=*html)))
"""

MSG = "Prepare for Release"

# values to parse from src files as null (n/a will be used for output)
NULLS = ["", "na", "n/a"]

# https://bids-specification.readthedocs.io/en/v1.9.0/common-principles.html#units
DATE_FORMAT = "%Y-%m-%d%H:%M:%S"

SIDECAR_FIELDS_TO_REMOVE = ["InstitutionAddress"]


def to_bids_tsv(
    tbl: ibis.expr.types.Table | pd.DataFrame,
    dst: Path,
    dt_columns: list[str] | None = None,
) -> None:
    if isinstance(tbl, ibis.expr.types.Table):
        d = tbl.execute()
    else:
        d = tbl.copy()
    if dt_columns:
        for dt_column in dt_columns:
            d[dt_column] = pd.to_datetime(d[dt_column])
    d.to_csv(
        dst,
        sep="\t",
        na_rep="n/a",
        index=False,
        date_format=DATE_FORMAT,
    )


def init_and_save(dataset: Path, n_jobs: int = 1) -> None:
    # initialize the repository
    dla.create(path=dataset, force=True)  # type: ignore
    (dataset / ".gitattributes").write_text(GITATTRIBUTES)

    logging.info(f"adding {dataset} files to annex")
    try:
        output = subprocess.run(
            ["git", "annex", "add", "-q", "-J", str(n_jobs), "."],
            check=True,
            cwd=dataset,
            capture_output=True,
        )
        logging.info(output)
    except subprocess.CalledProcessError as e:
        logging.error(e)

    logging.info(f"saving {dataset}")
    try:
        output = subprocess.run(
            ["git", "commit", "-m", MSG],
            check=True,
            cwd=dataset,
            capture_output=True,
        )
        logging.info(output)
    except subprocess.CalledProcessError as e:
        logging.error(e)


def update(dataset: Path, n_jobs: int = 1) -> None:
    dla.save(dataset=dataset, jobs=n_jobs)  # type: ignore


def archive_to_ria2(
    dataset: Path, archive_dir: Path, ria_sibling: str = "ria", n_jobs: int = 1
) -> None:
    # https://neurostars.org/t/what-is-the-preferred-strategy-for-creating-and-updating-an-archive-7z-in-a-ria-store/25683/5

    # push to RIA
    dla.push(dataset=dataset, to=ria_sibling, jobs=n_jobs)  # type: ignore

    # export archive (always calls 7z with update flag)
    dla.export_archive_ora(dataset=dataset, target=archive_dir)  # type: ignore


def _get_entity(f: Path, pattern: str) -> str:
    possibility = re.findall(pattern, str(f))
    if not len(possibility):
        raise ValueError
    return possibility[0]


def _get_sub(f: Path) -> str:
    return _get_entity(f=f, pattern=r"(?<=sub-)\d{5}")


def _get_ses(f: Path) -> str:
    return _get_entity(f=f, pattern=r"(?<=ses-)V[13]")


def _copy_if_needed(src, dst, *args, **kwargs) -> Path:  # noqa: ARG001
    if Path(dst).exists():
        logging.info(
            f"File {src} would overwrite {dst}. Leaving files unchanged."
        )
    elif (src2 := Path(src)).is_symlink():
        Path(dst).symlink_to(Path(src2).resolve())
    else:
        # otherwise, copy the file
        shutil.copy2(src, dst)
    return dst


def _symlink_if_needed(src, dst, *args, **kwargs) -> Path:  # noqa: ARG001
    if Path(dst).exists():
        logging.info(
            f"File {src} would overwrite {dst}. Leaving files unchanged."
        )
    else:
        Path(dst).symlink_to(Path(src).resolve())
    return dst


def write_participants(records: list[int], outdir: Path) -> None:
    demographics = (
        ibis.read_csv(datasets.get_demographics(), nullstr=NULLS)
        .select("record_id", "sex", "age", "dom_hand")
        .mutate(
            sex=_.sex.cases(
                ((1, "male"), (2, "female"), (3, "n/a"), (4, "other"))
            ),  # type: ignore
            handedness=_.dom_hand.cases(
                ((1, "right"), (2, "left"), (3, "ambidextrous")), default="n/a"
            ),  # type: ignore
        )
    )
    tbl = (
        ibis.read_csv(datasets.get_ilog(), nullstr=NULLS)
        .select("site", sub="subject_id", ses="visit")
        .filter(_.ses.contains("V1"))  # type: ignore
        .filter(_.sub.isin(records))  # type: ignore
        .join(demographics, _.sub == demographics.record_id)  # type: ignore
        .select("sub", "sex", "age", "handedness", "site")
    )
    to_bids_tsv(tbl, dst=outdir / "participants.tsv")

    shutil.copy2(datasets.get_participants_json(), outdir)


def write_sessions(outdir: Path) -> None:
    ilog = (
        ibis.read_csv(datasets.get_ilog(), nullstr=NULLS)
        .filter(_.visit.contains("V1"))  # type: ignore
        .select(
            session_id="visit",
            site="site",
            sub="subject_id",
            face_mask="Face Mask",
            magnet="Magnet Name",
            acquisition_week="acquisition_week",
            t1_tech_rating="fMRI T1 Tech Rating",
            cuff1_pressure_from_qst="Cuff1 QST Pressure",
            cuff1_recalibrated_pressure="Cuff1 Recalibrated Pressure",
            cuff1_applied_pressure="Cuff1 Applied Pressure",
            surgical_site_pain_rest="Surgical site pain rest",
            body_pain_rest="Body pain rest",
            surgical_site_pain_after_first_scan="Surgical site pain after first scan",
            body_pain_after_first_scan="Body pain after first scan",
            cuff_pain_after_first_scan="Cuff pain after first scan",
            cuff_pain_cuff1_beginning="Cuff pain cuff1 beginning",
            cuff_pain_cuff1_middle="Cuff pain cuff1 middle",
            cuff_pain_cuff1_end="Cuff pain cuff1 end",
            cuff_pain_cuff2_beginning="Cuff pain cuff2 beginning",
            cuff_pain_cuff2_middle="Cuff pain cuff2 middle",
            cuff_pain_cuff2_end="Cuff pain cuff2 end",
            cuff_pain_rest_beginning="Cuff pain rest beginning",
            cuff_pain_rest_middle="Cuff pain rest middle",
            cuff_pain_rest_end="Cuff pain rest end",
            surgical_site_pain_after_last_scan="Surgical site pain after last scan",
            body_pain_after_last_scan="Body pain after last scan",
            cuff_contraindicated="Cuff contraindicated",
            surgery_week="Surgery Week",
            cuff_leg="Cuff Leg",
        )
        .mutate(session_id=_.session_id.re_replace("V", "ses-V"))  # type: ignore
        .mutate(UM=_.magnet.cast(int).cast(str).cases((("1", "2"), ("2", "1")), default=""))  # type: ignore
        .mutate(scanner=_.site + _.UM)  # type: ignore
        .mutate(
            face_mask=_.face_mask.cast(bool),  # type: ignore
            cuff_contraindicated=_.cuff_contraindicated.cast(bool),  # type: ignore
        )  # type: ignore
        .drop("UM", "site", "magnet")
    )

    # look at parents of ses* dir rather than simply sub* because there may
    # be files that match sub* at the top level
    for sesdir in outdir.glob("sub*/ses*"):
        sub = int(_get_sub(sesdir))
        session = ilog.filter(_.sub == sub)  # type: ignore
        to_bids_tsv(
            session,
            dst=sesdir.parent / f"sub-{sub}_sessions.tsv",
            dt_columns=["acquisition_week", "surgery_week"],
        )

    shutil.copy2(datasets.get_sessions_json(), outdir / "sessions.json")


def update_scans(outdir: Path) -> None:
    qclog = ibis.read_csv(datasets.get_qclog(), nullstr=NULLS).select(
        "sub", "ses", "scan", "rating"
    )
    for scanstsv in outdir.rglob("*sub*scans.tsv"):
        scans = (
            ibis.read_csv(scanstsv, header=True, nullstr=NULLS)
            .select("filename")
            .mutate(
                sub=_.filename.re_extract(r"\d{5}", 0).cast("int64"),  # type: ignore
                ses=_.filename.re_extract("V1|V3", 0),  # type: ignore
                scan=_.filename.re_extract(
                    "fmap|anat|dwi|cuff_run-01|cuff_run-02|rest_run-01|rest_run-02",
                    0,
                ),  # type: ignore
            )
            .mutate(
                scan=_.scan.cases(
                    (
                        ("anat", "T1w"),
                        ("dwi", "DWI"),
                        ("cuff_run-01", "CUFF1"),
                        ("cuff_run-02", "CUFF2"),
                        ("rest_run-01", "REST1"),
                        ("rest_run-02", "REST2"),
                    ),
                    default=None,
                )  # type: ignore
            )
            .join(qclog, ("sub", "ses", "scan"), how="left")
            .select("filename", "rating")
        )
        to_bids_tsv(scans, scanstsv)
        if (scansjson := scanstsv.with_suffix(".json")).exists():
            scansjson.unlink()
    shutil.copy2(datasets.get_scans_json(), outdir / "scans.json")


def write_events(outdir: Path) -> None:
    pressure = ibis.read_csv(datasets.get_applied_pressures(), nullstr=NULLS)
    for nii in outdir.rglob("*bold.nii.gz"):
        fname = Path(str(nii.resolve()).replace("bold.nii.gz", "events.tsv"))
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
            pressure.filter(_.record_id == int(_get_sub(nii)))  # type: ignore
            .filter(_.visit == _get_ses(nii))  # type: ignore
            .filter(_.scan == scan)  # type: ignore
            .mutate(onset=0, duration=nb.load(nii).shape[-1])  # type: ignore
            .select("onset", "duration", "applied_pressure")
        )
        to_bids_tsv(events, fname)
    shutil.copy2(datasets.get_events_json(), outdir)


def write_readme(outdir: Path) -> None:
    shutil.copy2(datasets.get_readme(), outdir / "README")


def write_cat12_tables_and_jsons(
    inroot: Path, outroot: Path, records: list[int]
) -> None:
    df = (
        ibis.read_csv(
            inroot / "cat12" / "cluster_volumes.tsv",
            delim=r"\t",
            nullstr=NULLS,
        )
        .filter(_.ses.contains("V1"))  # type: ignore
        .filter(_.sub.isin(records))  # type: ignore
    )
    to_bids_tsv(df, outroot / "derivatives" / "cat12" / "cluster_volumes.tsv")
    shutil.copy2(
        datasets.get_cat12_json(),
        outroot / "derivatives" / "cat12" / "cluster_volumes.json",
    )


def write_freesurfer_tables_and_jsons(
    outroot: Path, inroot: Path, records: list[int]
) -> None:
    for tbl in ["aparc", "aseg", "headers"]:
        df = (
            ibis.read_csv(inroot / "freesurfer" / f"{tbl}.tsv", nullstr=NULLS)
            .filter(_.ses.contains("V1"))  # type: ignore
            .filter(_.sub.isin(records))  # type: ignore
        )
        to_bids_tsv(df, outroot / "derivatives" / "freesurfer" / f"{tbl}.tsv")

    shutil.copy2(
        datasets.get_aparc_json(),
        outroot / "derivatives" / "freesurfer" / "aparc.json",
    )
    shutil.copy2(
        datasets.get_aseg_json(),
        outroot / "derivatives" / "freesurfer" / "aseg.json",
    )
    shutil.copy2(
        datasets.get_headers_json(),
        outroot / "derivatives" / "freesurfer" / "headers.json",
    )


def write_fslanat_tables_and_jsons(
    inroot: Path, outroot: Path, records: list[int]
) -> None:
    df = (
        ibis.read_csv(inroot / "fslanat" / "fslanat.tsv", nullstr=NULLS)
        .filter(_.ses.contains("V1"))  # type: ignore
        .filter(_.sub.isin(records))  # type: ignore
    )
    to_bids_tsv(df, outroot / "derivatives" / "fslanat" / "fslanat.tsv")

    shutil.copy2(
        datasets.get_fslanat_json(),
        outroot / "derivatives" / "fslanat" / "fslanat.json",
    )


def write_fcn_jsons(outroot: Path) -> None:
    shutil.copy2(
        datasets.get_connectivity_acompcor_json(),
        outroot / "derivatives" / "fcn" / "acompcor.json",
    )
    shutil.copy2(
        datasets.get_connectivity_confounds_json(),
        outroot / "derivatives" / "fcn" / "connectivity-confounds.json",
    )
    shutil.copy2(
        datasets.get_connectivity_json(),
        outroot / "derivatives" / "fcn" / "connectivity.json",
    )


def write_signatures_jsons(outroot: Path) -> None:
    shutil.copy2(
        datasets.get_signature_by_part_json(),
        outroot / "derivatives" / "signatures" / "signature-by-part.json",
    )
    shutil.copy2(
        datasets.get_signature_by_run_json(),
        outroot / "derivatives" / "signatures" / "signature-by-run.json",
    )
    shutil.copy2(
        datasets.get_signature_by_tr_json(),
        outroot / "derivatives" / "signatures" / "signature-by-tr.json",
    )
    shutil.copy2(
        datasets.get_signature_confounds_json(),
        outroot / "derivatives" / "signatures" / "signature-confounds.json",
    )
    shutil.copy2(
        datasets.get_signature_labels_json(),
        outroot / "derivatives" / "signatures" / "signature-labels.json",
    )
    shutil.copy2(
        datasets.get_signature_rawdata_json(),
        outroot / "derivatives" / "signatures" / "signature-rawdata.json",
    )


def clean_sidecars(root: Path) -> None:
    for sidecar in root.rglob("*json"):
        data: dict[str, typing.Any] = json.loads(sidecar.read_text())
        for field in SIDECAR_FIELDS_TO_REMOVE:
            if data.get(field):
                del data[field]
        sidecar.write_text(json.dumps(data, indent=2, sort_keys=True))

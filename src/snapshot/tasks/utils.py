import json
import logging
import re
import shutil
import subprocess
import typing
from pathlib import Path

import datalad.api as dla
import ibis
import nibabel as nb
import pandas as pd
from ibis import _

from snapshot import datasets

GITATTRIBUTES = """
* annex.backend=MD5E
* annex.largefiles=(((mimeencoding=binary)and(largerthan=0))or((include=*gii)or(include=*svg)or(include=*html)))
"""

MSG = "Prepare for Release"


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


def _write_participants(records: list[int], outdir: Path) -> None:
    demographics = (
        ibis.read_csv(datasets.get_demographics())
        .select("record_id", "sex", "dom_hand")
        .mutate(
            sex=_.sex.cases(
                ((1, "male"), (2, "female"), (3, "n/a"), (4, "other"))
            ),  # type: ignore
            handedness=_.dom_hand.cases(
                ((1, "right"), (2, "left"), (3, "ambidextrous")), default="n/a"
            ),  # type: ignore
        )
    )
    (
        ibis.read_csv(datasets.get_ilog())
        .select("site", "subject_id", "visit", "Magnet Name")
        .filter(_.visit.contains("V1"))  # type: ignore
        .filter(_.subject_id.isin(records))  # type: ignore
        .relabel(
            {"Magnet Name": "magnet", "subject_id": "sub", "visit": "ses"}
        )
        .mutate(UM=_.magnet.cases((("1", "2"), ("2", "1")), default=""))  # type: ignore
        .mutate(scanner=_.site + _.UM)  # type: ignore
        .join(demographics, _.sub == demographics.record_id)  # type: ignore
        .select("sub", "scanner", "sex", "handedness")
        .execute()
    ).to_csv(
        outdir / "participants.tsv",
        sep="\t",
        na_rep="n/a",
        index=False,
    )
    shutil.copy2(datasets.get_participants_json(), outdir)


def _update_scans(outdir: Path) -> None:
    qclog = ibis.read_csv(datasets.get_qclog()).select(
        "sub", "ses", "scan", "rating"
    )
    for scanstsv in outdir.rglob("*sub*scans.tsv"):
        (
            ibis.read_csv(scanstsv, header=True)
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
            .execute()
        ).to_csv(scanstsv, sep="\t", index=False, na_rep="n/a")


def _write_events(outdir: Path) -> None:
    pressure = ibis.read_csv(datasets.get_applied_pressures())
    for nii in outdir.rglob("*bold.nii.gz"):
        fname = str(nii.resolve()).replace("bold.nii.gz", "events.tsv")
        if "cuff_run-01" in nii.name:
            scan = "CUFF1"
        elif "cuff_run-02" in nii.name:
            scan = "CUFF2"
        elif "rest" in nii.name:
            if (ev := Path(fname)).exists():
                ev.unlink()
            continue
        else:
            scan = ""
        pressure.filter(_.record_id == int(_get_sub(nii))).filter(  # type: ignore
            _.visit == _get_ses(nii)  # type: ignore
        ).filter(
            _.scan == scan  # type: ignore
        ).mutate(
            onset=0, duration=nb.load(nii).shape[-1]  # type: ignore
        ).select(
            "onset", "duration", "applied_pressure"
        ).execute().to_csv(
            fname, sep="\t", index=False, na_rep="n/a"
        )
    shutil.copy2(datasets.get_events_json(), outdir)


def _write_readme(outdir: Path) -> None:
    shutil.copy2(datasets.get_readme(), outdir / "README")


def write_freesurfer_tables_and_jsons(
    outroot: Path, inroot: Path, records: list[int]
) -> None:
    for tbl in ["aparc", "aseg", "headers"]:
        df: pd.DataFrame = (
            (ibis.read_csv(inroot / "freesurfer" / f"{tbl}.tsv"))
            .filter(_.ses.contains("V1"))  # type: ignore
            .filter(_.sub.isin(records))  # type: ignore
            .execute()
        )

        df.to_csv(
            outroot / "derivatives" / "freesurfer" / f"{tbl}.tsv",
            index=False,
            sep="\t",
        )

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
    df: pd.DataFrame = (
        (ibis.read_csv(inroot / "fslanat" / "fslanat.tsv"))
        .filter(_.ses.contains("V1"))  # type: ignore
        .filter(_.sub.isin(records))  # type: ignore
        .execute()
    )

    df.to_csv(
        outroot / "derivatives" / "fslanat" / "fslanat.tsv",
        index=False,
        sep="\t",
    )
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
        if data.get("InstitutionAddress"):
            del data["InstitutionAddress"]
        if data.get("InstitutionName"):
            del data["InstitutionName"]
        sidecar.write_text(json.dumps(data, indent=2))

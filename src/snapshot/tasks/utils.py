import logging
import os
import re
import shutil
from pathlib import Path

import datalad.api as dla
import ibis
import nibabel as nb
from ibis import _

from snapshot import datasets

GITATTRIBUTES = """
* annex.backend=MD5E
* annex.largefiles=(((mimeencoding=binary)and(largerthan=0))or((include=*gii)or(include=*svg)or(include=*html)))
"""

ILOG = Path(
    "/corral-secure/projects/A2CPS/community/reports/imaging/imaging-log-latest.csv"
)
QCLOG = Path(
    "/corral-secure/projects/A2CPS/community/reports/imaging/qc-log-latest.csv"
)
DEMOGRAPHICS = Path(
    "/corral-secure/projects/A2CPS/products/consortium-data/pre-surgery/demographics/demographics-2023-05-19.csv"
)

FSOUTPUTS = ("orig.mgz", "orig_nu.mgz", "T1.mgz")


def init_and_save(dataset: Path, n_jobs: int = 1) -> None:
    # initialize the repository
    dla.create(path=dataset, force=True)  # type: ignore
    (dataset / ".gitattributes").write_text(GITATTRIBUTES)

    # store files in repo
    dla.save(dataset=dataset, jobs=n_jobs)  # type: ignore


def update(dataset: Path, n_jobs: int = 1) -> None:
    dla.save(dataset=dataset, jobs=n_jobs)  # type: ignore


def add_ria(
    dataset: Path, alias: str, ria: str, ria_sibling: str = "ria"
) -> None:
    # create RIA
    dla.create_sibling_ria(  # type: ignore
        url=ria,
        name=ria_sibling,
        dataset=dataset,
        alias=alias,
        new_store_ok=True,
    )


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
    return _get_entity(f=f, pattern=r"\d{5}")


def _get_ses(f: Path) -> str:
    return _get_entity(f=f, pattern=r"V[13]")


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
        ibis.read_csv(DEMOGRAPHICS)
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
        ibis.read_csv(ILOG)
        .select("site", "subject_id", "visit", "Magnet Name")
        .filter(_.visit.contains("V1"))  # type: ignore
        .filter(_.subject_id.isin(records))  # type: ignore
        .relabel({"Magnet Name": "magnet", "subject_id": "sub", "visit": "ses"})
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


def _update_scans(outdir: Path) -> None:
    qclog = ibis.read_csv(QCLOG).select("sub", "ses", "scan", "rating")
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


def _prep_staged_dir(outroot: Path) -> None:
    # delete broken symlinks (e.g., files created by previous run of heudiconv that no longer exist)
    for target in os.walk(outroot):
        tar_dir = Path(target[0])
        for f in target[2]:
            if not (broken := tar_dir / f).exists():
                logging.warning(f"deleting broken symlink: {broken}")
                broken.unlink()

    # delete empty directories
    for target in os.walk(outroot, topdown=False):
        if (len(target[1] + target[2]) == 0) and (
            (to_del := Path(target[0])).name
            not in [
                "tmp",
                "bak",
                "trash",
            ]  # these folders from FreeSurfer are generally empty (and should be kept)
        ):
            logging.warning(f"deleting empty directory: {to_del}")
            os.removedirs(to_del)

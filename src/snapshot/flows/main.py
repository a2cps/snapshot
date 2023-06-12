import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Literal

import pandas as pd

from snapshot import datasets
from snapshot.flows import bids, cat12, fmriprep, freesurfer, mriqc, qsiprep
from snapshot.tasks import utils

SITE_LONG = {
    "NS": "NS_northshore",
    "UI": "UI_uic",
    "UC": "UC_uchicago",
    "UM": "UM_umichigan",
    "SH": "SH_spectrum_health",
    "WS": "WS_wayne_state",
}

FSOUTPUTS = ("orig.mgz", "orig_nu.mgz", "T1.mgz")
JOBS = ["bids", "fmriprep", "cat12", "mriqc", "qsiprep"]
ILOG = Path(
    "/corral-secure/projects/A2CPS/community/reports/imaging/imaging-log-latest.csv"
)


def _test_sub(
    subsesdir: Path,
    outroot: Path,
    inroot: Path,
    site_long: str,
) -> bool:
    sub = utils._get_sub(subsesdir)
    ses = utils._get_ses(subsesdir)
    not_already_processed = not all(
        (outroot / j / f"sub-{sub}" / f"ses-{ses}").exists()
        for j in [
            "bids",
            "fmriprep-anat",
            "fmriprep-cuff",
            "fmriprep-rest",
            "qsiprep",
            "mriqc",
        ]
    )
    not_already_processed2 = not all(
        (outroot / j / f"sub-{sub}_ses-{ses}").exists() for j in ["freesurfer"]
    )
    all_regular_outputs_not_empty = all(
        (jobdir := (inroot / site_long / j / subsesdir.name)).exists()
        and len(list(jobdir.iterdir()))
        for j in JOBS
    )
    all_subdirs_not_empty = all(
        (
            modalitydir := (inroot / site_long / j / subsesdir.name / modality)
        ).exists()
        and len(list(modalitydir.iterdir()))
        for j in ["mriqc", "fmriprep"]
        for modality in ["anat", "rest", "cuff"]
    )
    return (
        not_already_processed
        and not_already_processed2
        and all_regular_outputs_not_empty
        and all_subdirs_not_empty
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


def _deface(subsesdir: Path, tmp_site: Path) -> None:
    sub = utils._get_sub(subsesdir)
    ses = utils._get_ses(subsesdir)
    subses_fmriprep = tmp_site / "fmriprep" / subsesdir
    fmriprep_mask = (
        subses_fmriprep
        / "anat"
        / "fmriprep"
        / f"sub-{sub}"
        / f"ses-{ses}"
        / "anat"
        / f"sub-{sub}_ses-{ses}_desc-brain_mask.nii.gz"
    )
    utils._deface(
        tmp_site
        / "bids"
        / subsesdir
        / f"sub-{sub}"
        / f"ses-{ses}"
        / "anat"
        / f"sub-{sub}_ses-{ses}_T1w.nii.gz",
        fmriprep_mask,
    )
    for subjob in ["anat", "cuff", "rest"]:
        # output might not exist
        for output in (
            subses_fmriprep / subjob / "fmriprep" / f"sub-{sub}"
        ).glob("ses*"):
            utils._deface(
                output
                / "anat"
                / f"sub-{sub}_ses-{ses}_desc-preproc_T1w.nii.gz",
                fmriprep_mask,
            )

            utils._deface(
                output
                / "anat"
                / f"sub-{sub}_ses-{ses}_space-MNI152NLin2009cAsym_desc-preproc_T1w.nii.gz",
                output
                / "anat"
                / f"sub-{sub}_ses-{ses}_space-MNI152NLin2009cAsym_desc-brain_mask.nii.gz",
            )
    # now freesurfer
    utils._deface(
        subses_fmriprep
        / "anat"
        / "freesurfer"
        / f"sub-{sub}"
        / "mri"
        / "orig"
        / "001.mgz",
        fmriprep_mask,
    )
    utils._deface(
        subses_fmriprep
        / "anat"
        / "freesurfer"
        / f"sub-{sub}"
        / "mri"
        / "rawavg.mgz",
        fmriprep_mask,
    )
    for mgz in FSOUTPUTS:
        utils._deface(
            subses_fmriprep
            / "anat"
            / "freesurfer"
            / f"sub-{sub}"
            / "mri"
            / mgz,
            subses_fmriprep
            / "anat"
            / "freesurfer"
            / f"sub-{sub}"
            / "mri"
            / "brainmask.mgz",
            make_mask=True,
        )
    subses_qsiprep = tmp_site / "qsiprep" / subsesdir
    utils._deface(
        subses_qsiprep
        / "qsiprep"
        / f"sub-{sub}"
        / "anat"
        / f"sub-{sub}_desc-preproc_T1w.nii.gz",
        subses_qsiprep
        / "qsiprep"
        / f"sub-{sub}"
        / "anat"
        / f"sub-{sub}_desc-brain_mask.nii.gz",
    )
    utils._deface(
        subses_qsiprep
        / "qsiprep"
        / f"sub-{sub}"
        / "anat"
        / f"sub-{sub}_space-MNI152NLin2009cAsym_desc-preproc_T1w.nii.gz",
        subses_qsiprep
        / "qsiprep"
        / f"sub-{sub}"
        / "anat"
        / f"sub-{sub}_space-MNI152NLin2009cAsym_desc-brain_mask.nii.gz",
    )


def stage(inroot: Path, outroot: Path, max_subs: float | int = float("inf")):
    _prep_staged_dir(outroot=outroot)
    i = 0
    # only work with subs/sessions that have all jobs done (need fmriprep-anat for masking)
    # and only make copies of subs/sessions that do not already exist in outroot
    with tempfile.TemporaryDirectory() as tmpd:
        tmpdir = Path(tmpd)
        # Recursively create symlinks in the target directory
        for site_code, site_long in SITE_LONG.items():
            subses_tocopy: set[str] = set()
            for job in JOBS:
                in_job_dir = inroot / site_long / job

                # grab only sub/ses that do not already exist in output
                # and that have complete jobs
                for subsesdir in in_job_dir.glob(f"{site_code}*V[13]"):
                    if i >= max_subs:
                        continue
                    if _test_sub(
                        subsesdir=subsesdir,
                        outroot=outroot,
                        inroot=inroot,
                        site_long=site_long,
                    ):
                        subses_tocopy.add(subsesdir.name)
                        i += 1

            tmp_site = tmpdir / site_long
            for subsesd in subses_tocopy:
                subsesdir = Path(subsesd)
                for job in JOBS:
                    out_job_dir = tmp_site / job
                    outsubses = out_job_dir / subsesdir
                    shutil.copytree(
                        inroot / site_long / job / subsesdir,
                        outsubses,
                        copy_function=utils._symlink_if_needed,
                        ignore=shutil.ignore_patterns(
                            "work", "*_wf", "sourcedata"
                        ),
                    )

                # mask all images
                _deface(subsesdir=subsesdir, tmp_site=tmp_site)

            # now aggregate all new participants
            bids.main(inroot=tmp_site, outdir=outroot / "bids")
            cat12.main(inroot=tmp_site, outdir=outroot / "cat12")
            qsiprep.main(inroot=tmp_site, outdir=outroot / "qsiprep")
            mriqc.main(inroot=tmp_site, outdir=outroot / "mriqc")
            fmriprep.main(inroot=tmp_site, outdir=outroot / "fmriprep-anat")
            fmriprep.main(inroot=tmp_site, outdir=outroot / "fmriprep-cuff")
            fmriprep.main(inroot=tmp_site, outdir=outroot / "fmriprep-rest")
            freesurfer.main(inroot=tmp_site, outdir=outroot / "freesurfer")


def add_ria(
    releasedir: Path,
    store: tuple[
        Literal[
            "rawdata",
            "cat12",
            "qsiprep",
            "mriqc",
            "fmriprep-anat",
            "fmriprep-cuff",
            "fmriprep-rest",
            "freesurfer",
        ],
        ...,
    ],
    ria: str,
):
    for s in store:
        utils.add_ria(dataset=releasedir / s, alias=s, ria=ria)


def archive(
    releasedir: Path,
    store: tuple[
        Literal[
            "rawdata",
            "cat12",
            "qsiprep",
            "mriqc",
            "fmriprep-anat",
            "fmriprep-cuff",
            "fmriprep-rest",
            "freesurfer",
        ],
        ...,
    ],
    ria: Path,
    n_jobs: int = 1,
):
    for s in store:
        ria_dir = ria / "alias" / s
        utils.archive_to_ria2(
            dataset=releasedir / s,
            archive_dir=ria_dir / "archives" / "archive.7z",
            n_jobs=n_jobs,
        )
        shutil.rmtree(ria_dir / "annex" / "objects")


def releasev1(inroot: Path, outroot: Path) -> None:
    records = datasets.get_v1_recordids()
    for job in [
        "bids",
        "cat12",
        "qsiprep",
        "mriqc",
        "fmriprep-anat",
        "fmriprep-cuff",
        "fmriprep-rest",
        "freesurfer",
    ]:
        injobdir = inroot / job

        # get list of subjects that are present in the input
        # directory but which won't be included in release

        subs_to_exclude = set()
        for file in injobdir.rglob("sub-*"):
            sub = int(utils._get_sub(file))
            if sub not in records:
                subs_to_exclude.add(sub)

        # copy all files from input directory, except those excluded subs
        # and any V3 data
        shutil.copytree(
            injobdir,
            outroot / job,
            ignore=shutil.ignore_patterns(
                "*ses-V3*", [f"*sub-{sub}*" for sub in subs_to_exclude]
            ),
        )

    # handle top-level stuff
    participants = pd.read_csv(
        ILOG, usecols=["site", "subject_id", "visit", "Magnet Name"]
    )

import shutil
import tempfile
from pathlib import Path
from typing import Literal

# from dask import config
# import prefect_dask
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


def stage(inroot: Path, outroot: Path, max_subs: float | int = float("inf")):
    jobs = ["bids", "fmriprep", "cat12", "mriqc", "qsiprep"]
    i = 0
    # only work with subs/sessions that have all jobs done (need fmriprep-anat for masking)
    # and only make copies of subs/sessions that do not already exist in outroot
    with tempfile.TemporaryDirectory() as tmpd:
        tmpdir = Path(tmpd)
        # Recursively create symlinks in the target directory
        for site_code, site_long in SITE_LONG.items():
            subses_tocopy: set[str] = set()
            for job in jobs:
                in_job_dir = inroot / site_long / job

                # grab only sub/ses that do not already exist in output
                # and that have complete jobs
                for subsesdir in in_job_dir.glob(f"{site_code}*V[13]"):
                    if i >= max_subs:
                        continue
                    sub = utils._get_sub(subsesdir)
                    ses = utils._get_ses(subsesdir)
                    if (
                        not (
                            outroot / "bids" / f"sub-{sub}" / f"ses-{ses}"
                        ).exists()
                    ) and all(
                        (inroot / site_long / j / subsesdir.name).exists()
                        for j in jobs
                    ):
                        subses_tocopy.add(subsesdir.name)
                        i += 1

            tmp_site = tmpdir / site_long
            for subsesd in subses_tocopy:
                subsesdir = Path(subsesd)
                for job in jobs:
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

import shutil
import tempfile
from pathlib import Path

from snapshot.flows import (
    bids_wf,
    cat12_wf,
    fmriprep_wf,
    freesurfer_wf,
    fslanat_wf,
    mriqc_wf,
    qsiprep_wf,
)
from snapshot.tasks import utils

SITE_LONG = {
    "NS": "NS_northshore",
    "UI": "UI_uic",
    "UC": "UC_uchicago",
    "UM": "UM_umichigan",
    "SH": "SH_spectrum_health",
    "WS": "WS_wayne_state",
}

JOBS = ["bids", "fmriprep", "cat12", "mriqc", "qsiprep", "fslanat"]


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
    not_already_processed_fs = not (
        (outroot / "freesurfer" / f"sub-{sub}_ses-{ses}").exists()
    )
    not_already_processed_cat = not (
        (
            outroot
            / "cat12"
            / "report"
            / f"catreport_sub-{sub}_ses-{ses}_T1w.pdf"
        ).exists()
    )

    not_already_processed_fslanat = not (
        (outroot / "fslanat" / f"sub-{sub}_ses-{ses}.anat").exists()
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
        and not_already_processed_fs
        and not_already_processed_cat
        and not_already_processed_fslanat
        and all_regular_outputs_not_empty
        and all_subdirs_not_empty
    )


def main(inroot: Path, outroot: Path, max_subs: float | int = float("inf")):
    utils._prep_staged_dir(outroot=outroot)
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
                utils._deface_all(subsesdir=subsesdir, tmp_site=tmp_site)

            # now aggregate all new participants
            # testing for len(subses_tocopy) to handle cases where no participants
            # were copied into the ouptut directory (e.g., during testing)
            if len(subses_tocopy):
                bids_wf.main(inroot=tmp_site, outdir=outroot / "bids")
                cat12_wf.main(inroot=tmp_site, outdir=outroot / "cat12")
                qsiprep_wf.main(inroot=tmp_site, outdir=outroot / "qsiprep")
                mriqc_wf.main(inroot=tmp_site, outdir=outroot / "mriqc")
                fmriprep_wf.main(
                    inroot=tmp_site, outdir=outroot / "fmriprep-anat"
                )
                fmriprep_wf.main(
                    inroot=tmp_site, outdir=outroot / "fmriprep-cuff"
                )
                fmriprep_wf.main(
                    inroot=tmp_site, outdir=outroot / "fmriprep-rest"
                )
                freesurfer_wf.main(
                    inroot=tmp_site, outdir=outroot / "freesurfer"
                )
                fslanat_wf.main(inroot=tmp_site, outdir=outroot / "fslanat")

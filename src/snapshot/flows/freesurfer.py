import shutil
from pathlib import Path

from snapshot.tasks import utils

ALIAS = "freesurfer"

SITE_LONG = {
    "NS": "NS_northshore",
    "UI": "UI_uic",
    "UC": "UC_uchicago",
    "UM": "UM_umichigan",
    "SH": "SH_spectrum_health",
    "WS": "WS_wayne_state",
}


def main(outdir: Path, inroot: Path, ria: str | None = None, n_jobs: int = 1) -> None:
    if not outdir.exists():
        outdir.mkdir(parents=True)

    for site, site_long in SITE_LONG.items():
        for src in inroot.glob(f"{site_long}/fmriprep/{site}*/anat/freesurfer/sub*"):
            # folders renamed so that sessions do not collide
            shutil.copytree(src, outdir / f"sub-{utils._get_sub(src)}_ses-{utils._get_ses(src)}")

    utils.create_save_and_ria(dataset=outdir, ria=ria, alias=ALIAS, n_jobs=n_jobs)

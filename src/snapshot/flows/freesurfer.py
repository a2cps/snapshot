import shutil
from pathlib import Path
from typing import Literal

from snapshot.tasks import utils

SITE_LONG = {
    "NS": "NS_northshore",
    "UI": "UI_uic",
    "UC": "UC_uchicago",
    "UM": "UM_umichigan",
    "SH": "SH_spectrum_health",
    "WS": "WS_wayne_state",
}


def main(outdir: Path, inroot: Path, action: Literal["init", "update"], n_jobs: int = 1) -> None:
    if not outdir.exists():
        outdir.mkdir(parents=True)

    for site, site_long in SITE_LONG.items():
        for src in inroot.glob(f"{site_long}/fmriprep/{site}*/anat/freesurfer/sub*"):
            # folders renamed so that sessions do not collide
            shutil.copytree(
                src,
                outdir / f"sub-{utils._get_sub(src)}_ses-{utils._get_ses(src)}",
                copy_function=utils._copy_if_needed,
            )

    match action:
        case "init":
            utils.init_and_save(dataset=outdir, n_jobs=n_jobs)
        case "update":
            utils.update(dataset=outdir, n_jobs=n_jobs)

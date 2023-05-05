import shutil
from pathlib import Path

from snapshot.tasks import utils

ALIAS = "cat12"

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

    # copy files over
    for site, site_long in SITE_LONG.items():
        for src in inroot.glob(f"{site_long}/cat12/{site}*"):
            for out in ["label", "mri", "report", "surf"]:
                shutil.copytree(src / out, outdir / out, dirs_exist_ok=True)

    utils.create_save_and_ria(dataset=outdir, ria=ria, alias=ALIAS, n_jobs=n_jobs)

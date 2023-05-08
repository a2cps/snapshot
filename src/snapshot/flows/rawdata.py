import json
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

BIDS_IGNORE = """
*.err
*.out
__pycache__*
sub-*/ses-*/fmap/*.bval
sub-*/ses-*/fmap/*.bvec
"""

DESCRIPTION = {"BIDSVersion": "1.8.0", "Name": "A2CPS"}

README = "A2CPS dataset"


def main(outdir: Path, inroot: Path, action: Literal["init", "update"], n_jobs: int = 1) -> None:
    if not outdir.exists():
        outdir.mkdir(parents=True)

    # copy files over
    for site in SITE_LONG.values():
        for bids in inroot.glob(f"{site}/bids/*"):
            for src_id in bids.glob("sub-*"):
                shutil.copytree(src_id, outdir / src_id.name, dirs_exist_ok=True, copy_function=utils._copy_if_needed)

    # create top-level files
    readme = outdir / "README"
    readme.touch()
    readme.write_text(README)

    description = outdir / "dataset_description.json"
    description.write_text(json.dumps(DESCRIPTION, indent=2))

    bids_ignore = outdir / ".bidsignore"
    bids_ignore.write_text(BIDS_IGNORE)

    match action:
        case "init":
            utils.init_and_save(dataset=outdir, n_jobs=n_jobs)
        case "update":
            utils.update(dataset=outdir, n_jobs=n_jobs)

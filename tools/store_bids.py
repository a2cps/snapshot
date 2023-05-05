import argparse
import json
import shutil
from pathlib import Path

import datalad.api as dla

PROJECT = Path("/corral-secure/projects/A2CPS")
# ROOT = PROJECT / "products" / "mris" / "all_sites"
ROOT = PROJECT / "shared" / "psadil" / "jobs" / "release" / "products"
ALIAS = "rawdata"
TARGET = ROOT / ALIAS
# SRC = ROOT / "bids"
SRC = PROJECT / "products" / "mris" / "all_sites"
# RIA = f"ria+file:///{PROJECT}/products/mris/all_sites/ria"
RIA = f"ria+file://{ROOT}/ria"

RIA_SIBLING = "ria"
RIA_STORE = "ria-store"

N_JOBS = 1

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

DESCRIPTION = {
    "Acknowledgements": "TODO",
    "Authors": ["TODO:"],
    "BIDSVersion": "1.4.1",
    "DatasetDOI": "TODO",
    "Funding": ["TODO"],
    "HowToAcknowledge": "TODO",
    "License": "TODO",
    "Name": "A2CPS",
    "ReferencesAndLinks": ["TODO"],
}

README = "A2CPS dataset"


def main(outdir: Path, inroot: Path) -> None:
    if not outdir.exists():
        outdir.mkdir(parents=True)

    # copy files over
    for site in SITE_LONG.values():
        for bids in inroot.glob(f"{site}/bids/*"):
            for src_id in bids.glob("sub-*"):
                shutil.copytree(src_id, outdir / src_id.name, dirs_exist_ok=True)
            break

    # create top-level files
    readme = outdir / "README"
    readme.touch()
    readme.write_text(README)

    description = outdir / "dataset_description.json"
    description.write_text(json.dumps(DESCRIPTION, indent=2))

    bids_ignore = outdir / ".bidsignore"
    bids_ignore.write_text(BIDS_IGNORE)

    # initialize the repository
    dla.create(path=outdir, force=True, cfg_proc="text2git")

    # store files in repo
    dla.save(dataset=outdir, version_tag="0.1", jobs=N_JOBS)

    # create RIA
    dla.create_sibling_ria(
        url=RIA,
        name=RIA_SIBLING,
        dataset=outdir,
        alias=ALIAS,
        storage_name=RIA_STORE,
        new_store_ok=True,
    )

    # push to RIA
    dla.push(dataset=outdir, to=RIA_SIBLING, jobs=N_JOBS)


if __name__ == "__main__":
    """
    Example script for aggregating a dataset, initializing datalad, and storing in RIA
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("inroot", type=Path)
    parser.add_argument("outdir", type=Path)

    args = parser.parse_args()
    main(inroot=args.inroot, outdir=args.outdir)

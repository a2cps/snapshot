import json
import re
import shutil
from pathlib import Path
from typing import Literal

from snapshot import datasets
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
*.html
logs/
figures/
*_xfm.*
*.surf.gii
*_boldref.nii.gz
*_bold.func.gii
*_mixing.tsv
*_AROMAnoiseICs.csv
*_timeseries.tsv
"""


DESCRIPTION = {
    "Name": "fMRIPrep - fMRI PREProcessing workflow",
    "BIDSVersion": "1.8.0",
    "DatasetType": "derivative",
    "GeneratedBy": [
        {
            "Name": "fMRIPrep",
            "Version": "20.2.3",
            "CodeURL": "https://github.com/nipreps/fmriprep/archive/20.2.3.tar.gz",
        }
    ],
    "SourceDatasets": [
        {
            "URL": "https://doi.org/TODO: eventually a DOI for the dataset",
            "DOI": "TODO: eventually a DOI for the dataset",
        }
    ],
}


README = "A2CPS dataset"


def main(outdir: Path, inroot: Path, action: Literal["init", "update"], n_jobs: int = 1) -> None:
    # copy files over

    _job = re.findall(r"(?<=fmriprep-)(anat|cuff|rest)", str(outdir))
    if not _job:
        raise ValueError
    job = _job[0]
    if not outdir.exists():
        outdir.mkdir(parents=True)

    for site, site_long in SITE_LONG.items():
        # this grabs both sub-##### directories and sub*html files
        for src in inroot.glob(f"{site_long}/fmriprep/{site}*/{job}/fmriprep/sub*"):
            if src.is_file():
                utils._copy_if_needed(src, outdir)
            else:
                shutil.copytree(
                    src, outdir / src.name, dirs_exist_ok=True, copy_function=utils._copy_if_needed
                )

    # create top-level files
    readme = outdir / "README"
    readme.touch()
    readme.write_text(README)

    description = outdir / "dataset_description.json"
    description.write_text(json.dumps(DESCRIPTION, indent=2))

    bids_ignore = outdir / ".bidsignore"
    bids_ignore.write_text(BIDS_IGNORE)

    shutil.copy2(datasets.get_aseg(), outdir)
    shutil.copy2(datasets.get_aparcaseg(), outdir)

    match action:
        case "init":
            utils.init_and_save(dataset=outdir, n_jobs=n_jobs)
        case "update":
            utils.update(dataset=outdir, n_jobs=n_jobs)

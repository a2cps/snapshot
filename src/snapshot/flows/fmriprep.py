import json
import shutil
from pathlib import Path

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


def main(outdir: Path, inroot: Path, ria: str | None = None, n_jobs: int = 1) -> None:
    # copy files over
    for job in ["anat", "cuff", "rest"]:
        _job = f"fmriprep-{job}"
        destination = outdir.with_name(_job)
        if not destination.exists():
            destination.mkdir(parents=True)

        for site, site_long in SITE_LONG.items():
            # this grabs both sub-##### directories and sub*html files
            for src in inroot.glob(f"{site_long}/fmriprep/{site}*/{job}/fmriprep/sub*"):
                if src.is_file():
                    shutil.copy2(src, destination)
                else:
                    shutil.copytree(src, destination / src.name, dirs_exist_ok=True)

        # create top-level files
        readme = destination / "README"
        readme.touch()
        readme.write_text(README)

        description = destination / "dataset_description.json"
        description.write_text(json.dumps(DESCRIPTION, indent=2))

        bids_ignore = destination / ".bidsignore"
        bids_ignore.write_text(BIDS_IGNORE)

        shutil.copy2(datasets.get_aseg(), destination)
        shutil.copy2(datasets.get_aparcaseg(), destination)

        utils.create_save_and_ria(dataset=destination, ria=ria, alias=_job, n_jobs=n_jobs)

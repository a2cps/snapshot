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

JOBS = ["bids", "fmriprep", "cat12", "mriqc", "qsiprep"]


def main(
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

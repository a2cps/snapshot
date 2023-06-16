from pathlib import Path
from typing import Literal

from snapshot.tasks import utils


def main(
    inroot: Path,
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
    n_jobs: int = 1,
) -> None:
    for s in store:
        utils.init_and_save(inroot / s, n_jobs=n_jobs)

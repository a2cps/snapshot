from pathlib import Path
from typing import Literal

from snapshot.tasks import utils


def add_ria(
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
    ria: str,
):
    for s in store:
        utils.add_ria(dataset=releasedir / s, alias=s, ria=ria)

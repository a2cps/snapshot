from typing import Literal

STORE_DIR = (
    "bids",
    "cat12",
    "mriqc",
    "fmriprep-anat",
    "fmriprep-cuff",
    "fmriprep-rest",
    "freesurfer",
)

store = Literal[
    "rawdata",
    "cat12",
    "mriqc",
    "fmriprep-anat",
    "fmriprep-cuff",
    "fmriprep-rest",
    "freesurfer",
]

from typing import Literal

STORE_DIR = (
    "rawdata",
    "cat12",
    "qsiprep",
    "mriqc",
    "fmriprep-anat",
    "fmriprep-cuff",
    "fmriprep-rest",
    "freesurfer",
)

store = Literal[
    "rawdata",
    "cat12",
    "qsiprep",
    "mriqc",
    "fmriprep-anat",
    "fmriprep-cuff",
    "fmriprep-rest",
    "freesurfer",
]

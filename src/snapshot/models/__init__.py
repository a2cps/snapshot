from typing import Literal

STORE_DIR = (
    "bids",
    "cat12",
    "mriqc",
    "fmriprep-anat",
    "fmriprep-cuff",
    "fmriprep-rest",
    "freesurfer",
    "fcn",
    "signatures",
)

store = Literal[
    "rawdata",
    "cat12",
    "mriqc",
    "fmriprep-anat",
    "fmriprep-cuff",
    "fmriprep-rest",
    "freesurfer",
    "fcn",
    "signatures",
]

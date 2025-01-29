import typing

type STORE_DIR = typing.Literal[
    "bids",
    "brainager",
    "cat12",
    "eddyqc",
    "fcn",
    "fmriprep",
    "freesurfer",
    "fslanat",
    "gift",
    "mriqc",
    "qsiprep-V1",
    "signatures",
]

STORE_DIRS = typing.get_args(STORE_DIR)

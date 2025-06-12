import typing

STORE_DIR = typing.Literal[
    "bedpostx",
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
    "postdtifit",
    "postgift",
    "qsiprep-V1",
    "qsirecon_fsl_dtifit",
    "signatures",
    "synthstrip",
]

STORE_DIRS = typing.get_args(STORE_DIR)

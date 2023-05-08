import shutil
from pathlib import Path
from typing import Literal

from snapshot.flows import cat12, fmriprep, freesurfer, mriqc, qsiprep, rawdata
from snapshot.tasks import utils


def init(
    inroot: Path,
    outdir: Path,
    store: tuple[
        Literal[
            "rawdata", "cat12", "qsiprep", "mriqc", "fmriprep-anat", "fmriprep-cuff", "fmriprep-rest", "freesurfer"
        ],
        ...,
    ],
    n_jobs: int = 1,
):
    action = "init"
    if "rawdata" in store:
        rawdata.main(inroot=inroot, outdir=outdir / "rawdata", n_jobs=n_jobs, action=action)
    if "cat12" in store:
        cat12.main(inroot=inroot, outdir=outdir / "cat12", n_jobs=n_jobs, action=action)
    if "qsiprep" in store:
        qsiprep.main(inroot=inroot, outdir=outdir / "qsiprep", n_jobs=n_jobs, action=action)
    if "mriqc" in store:
        mriqc.main(inroot=inroot, outdir=outdir / "mriqc", n_jobs=n_jobs, action=action)
    if "fmriprep-anat" in store:
        fmriprep.main(inroot=inroot, outdir=outdir / "fmriprep-anat", n_jobs=n_jobs, action=action)
    if "fmriprep-cuff" in store:
        fmriprep.main(inroot=inroot, outdir=outdir / "fmriprep-cuff", n_jobs=n_jobs, action=action)
    if "fmriprep-rest" in store:
        fmriprep.main(inroot=inroot, outdir=outdir / "fmriprep-rest", n_jobs=n_jobs, action=action)
    if "freesurfer" in store:
        freesurfer.main(inroot=inroot, outdir=outdir / "freesurfer", n_jobs=n_jobs, action=action)


def update(
    inroot: Path,
    outdir: Path,
    store: tuple[
        Literal[
            "rawdata", "cat12", "qsiprep", "mriqc", "fmriprep-anat", "fmriprep-cuff", "fmriprep-rest", "freesurfer"
        ],
        ...,
    ],
    n_jobs: int = 1,
):
    action = "update"
    if "rawdata" in store:
        rawdata.main(inroot=inroot, outdir=outdir / "rawdata", n_jobs=n_jobs, action=action)
    if "cat12" in store:
        cat12.main(inroot=inroot, outdir=outdir / "cat12", n_jobs=n_jobs, action=action)
    if "qsiprep" in store:
        qsiprep.main(inroot=inroot, outdir=outdir / "qsiprep", n_jobs=n_jobs, action=action)
    if "mriqc" in store:
        mriqc.main(inroot=inroot, outdir=outdir / "mriqc", n_jobs=n_jobs, action=action)
    if "fmriprep-anat" in store:
        fmriprep.main(inroot=inroot, outdir=outdir / "fmriprep-anat", n_jobs=n_jobs, action=action)
    if "fmriprep-cuff" in store:
        fmriprep.main(inroot=inroot, outdir=outdir / "fmriprep-cuff", n_jobs=n_jobs, action=action)
    if "fmriprep-rest" in store:
        fmriprep.main(inroot=inroot, outdir=outdir / "fmriprep-rest", n_jobs=n_jobs, action=action)
    if "freesurfer" in store:
        freesurfer.main(inroot=inroot, outdir=outdir / "freesurfer", n_jobs=n_jobs, action=action)


def add_ria(
    releasedir: Path,
    store: tuple[
        Literal[
            "rawdata", "cat12", "qsiprep", "mriqc", "fmriprep-anat", "fmriprep-cuff", "fmriprep-rest", "freesurfer"
        ],
        ...,
    ],
    ria: str,
):
    for s in store:
        utils.add_ria(dataset=releasedir / s, alias=s, ria=ria)


def archive(
    releasedir: Path,
    store: tuple[
        Literal[
            "rawdata", "cat12", "qsiprep", "mriqc", "fmriprep-anat", "fmriprep-cuff", "fmriprep-rest", "freesurfer"
        ],
        ...,
    ],
    ria: Path,
    n_jobs: int = 1,
):
    for s in store:
        ria_dir = ria / "alias" / s
        utils.archive_to_ria2(dataset=releasedir / s, archive_dir=ria_dir / "archives" / "archive.7z", n_jobs=n_jobs)
        shutil.rmtree(ria_dir / "annex" / "objects")

from pathlib import Path
from typing import Literal

import click

from snapshot import __version__
from snapshot.flows import main


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(version=__version__, prog_name="snapshot")
def snapshot() -> None:
    pass


@snapshot.command()
@click.argument("inroot", type=click.Path(exists=True, file_okay=False, resolve_path=True, path_type=Path))
@click.argument("outdir", type=click.Path(exists=False, file_okay=False, resolve_path=True, path_type=Path))
@click.argument(
    "store",
    nargs=-1,
    type=click.Choice(
        ("rawdata", "cat12", "fmriprep-anat", "fmriprep-cuff", "fmriprep-rest", "qsiprep", "mriqc", "freesurfer")
    ),
)
@click.option("--n-jobs", type=click.IntRange(min=1, min_open=False), default=1)
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
) -> None:
    """
    build \
        --n-jobs 10 \
        ./products/mris ./release rawdata
    """
    main.init(inroot=inroot, outdir=outdir, store=store, n_jobs=n_jobs)


@snapshot.command()
@click.argument("inroot", type=click.Path(exists=True, file_okay=False, resolve_path=True, path_type=Path))
@click.argument("outdir", type=click.Path(exists=False, file_okay=False, resolve_path=True, path_type=Path))
@click.argument(
    "store",
    nargs=-1,
    type=click.Choice(
        ("rawdata", "cat12", "fmriprep-anat", "fmriprep-cuff", "fmriprep-rest", "qsiprep", "mriqc", "freesurfer")
    ),
)
@click.option("--n-jobs", type=click.IntRange(min=1, min_open=False), default=1)
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
) -> None:
    main.update(inroot=inroot, outdir=outdir, store=store, n_jobs=n_jobs)


@snapshot.command()
@click.argument("releasedir", type=click.Path(exists=False, file_okay=False, resolve_path=True, path_type=Path))
@click.argument("ria", type=click.Path(exists=False, file_okay=False, path_type=str))
@click.argument(
    "store",
    nargs=-1,
    type=click.Choice(
        ("rawdata", "cat12", "fmriprep-anat", "fmriprep-cuff", "fmriprep-rest", "qsiprep", "mriqc", "freesurfer")
    ),
)
def add_ria(
    releasedir: Path,
    ria: str,
    store: tuple[
        Literal[
            "rawdata", "cat12", "qsiprep", "mriqc", "fmriprep-anat", "fmriprep-cuff", "fmriprep-rest", "freesurfer"
        ],
        ...,
    ],
) -> None:
    """
    snapshot add_ria \
        ./release \
        ria+file:///corral-secure/projects/A2CPS/shared/psadil/jobs/release/ria \
        rawdata
    """
    if ria and not ria.startswith("ria+"):
        msg = f"ria must begin with ria+, found {ria}"
        raise ValueError(msg)
    main.add_ria(releasedir=releasedir, ria=ria, store=store)


@snapshot.command()
@click.argument("releasedir", type=click.Path(exists=False, file_okay=False, resolve_path=True, path_type=Path))
@click.argument("ria", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.argument(
    "store",
    nargs=-1,
    type=click.Choice(
        ("rawdata", "cat12", "fmriprep-anat", "fmriprep-cuff", "fmriprep-rest", "qsiprep", "mriqc", "freesurfer")
    ),
)
@click.option("--n-jobs", type=click.IntRange(min=1, min_open=True), default=1)
def archive(
    releasedir: Path,
    ria: Path,
    store: tuple[
        Literal[
            "rawdata", "cat12", "qsiprep", "mriqc", "fmriprep-anat", "fmriprep-cuff", "fmriprep-rest", "freesurfer"
        ],
        ...,
    ],
    n_jobs: int = 1,
) -> None:
    main.archive(releasedir=releasedir, ria=ria, store=store, n_jobs=n_jobs)

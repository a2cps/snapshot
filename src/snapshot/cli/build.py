from pathlib import Path
from typing import Literal

import click

from snapshot import __version__
from snapshot.flows import main


@click.group(context_settings={"help_option_names": ["-h", "--help"]}, invoke_without_command=True)
@click.version_option(version=__version__, prog_name="build")
@click.argument("inroot", type=click.Path(exists=True, file_okay=False, resolve_path=True, path_type=Path))
@click.argument("outdir", type=click.Path(exists=False, file_okay=False, resolve_path=True, path_type=Path))
@click.argument(
    "store", nargs=-1, type=click.Choice(("rawdata", "cat12", "fmriprep", "qsiprep", "mriqc", "freesurfer"))
)
@click.option("--ria", type=click.Path(exists=False, file_okay=False, path_type=str))
@click.option("--n-jobs", type=click.IntRange(min=1, min_open=True), default=1)
def build(
    inroot: Path,
    outdir: Path,
    store: tuple[Literal["rawdata", "cat12", "qsiprep", "mriqc", "fmriprep", "freesurfer"], ...],
    ria: str | None = None,
    n_jobs: int = 1,
):
    """
    build \
        --n-jobs 10 \
        --ria ria+file:///corral-secure/projects/A2CPS/shared/psadil/jobs/release/ria \
        ./products/mris ./release rawdata
    """
    if ria and not ria.startswith("ria+"):
        msg = f"ria must begin with ria+, found {ria}"
        raise ValueError(msg)
    main.main(inroot=inroot, outdir=outdir, ria=ria, store=store, n_jobs=n_jobs)

from pathlib import Path

import click

from snapshot.flows import (
    add_ria_wf,
    archive_wf,
    copy_v1_to_dst_wf,
    init_datalad_wf,
)


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def main() -> None:
    pass


@main.command()
@click.argument(
    "inroot",
    type=click.Path(
        exists=True, file_okay=False, resolve_path=True, path_type=Path
    ),
)
@click.argument(
    "outroot",
    type=click.Path(
        exists=False, file_okay=False, resolve_path=True, path_type=Path
    ),
)
def copy_v1_to_dst(inroot: Path, outroot: Path) -> None:
    copy_v1_to_dst_wf.main(inroot=inroot, outroot=outroot)


@main.command()
@click.option("--n-jobs", type=int, default=1)
@click.argument(
    "releasedir",
    type=click.Path(
        exists=True, file_okay=False, resolve_path=True, path_type=Path
    ),
)
def init_datalad(releasedir: Path, n_jobs: int = 1) -> None:
    """
    snapshot init_datalad /corral-secure/projects/A2CPS/products/consortium-data/pre-surgery/mris
    """
    init_datalad_wf.main(inroot=releasedir, n_jobs=n_jobs)


@main.command()
@click.argument(
    "releasedir",
    type=click.Path(
        exists=True, file_okay=False, resolve_path=True, path_type=Path
    ),
)
@click.argument(
    "ria", type=click.Path(exists=False, file_okay=False, path_type=str)
)
def add_ria(releasedir: Path, ria: str) -> None:
    """
    snapshot add_ria \
        /corral-secure/projects/A2CPS/products/consortium-data/pre-surgery/mris \
        ria+file:///corral-secure/projects/A2CPS/products/consortium-data/pre-surgery/mris/ria 
    """
    if not ria.startswith("ria+"):
        msg = f"ria must begin with ria+, found {ria}"
        raise ValueError(msg)
    add_ria_wf.main(releasedir=releasedir, ria=ria)


@main.command()
@click.argument(
    "releasedir",
    type=click.Path(
        exists=True, file_okay=False, resolve_path=True, path_type=Path
    ),
)
@click.argument(
    "ria", type=click.Path(exists=True, file_okay=False, path_type=Path)
)
@click.option("--n-jobs", type=click.IntRange(min=1, min_open=True), default=1)
def archive(releasedir: Path, ria: Path, n_jobs: int = 1) -> None:
    """
    snapshot add_ria \
        /corral-secure/projects/A2CPS/products/consortium-data/pre-surgery/mris \
        /corral-secure/projects/A2CPS/products/consortium-data/pre-surgery/mris/ria 
    """
    archive_wf.main(releasedir=releasedir, ria=ria, n_jobs=n_jobs)

from pathlib import Path

import click

from snapshot import models
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
        exists=False, file_okay=False, resolve_path=True, path_type=Path
    ),
)
@click.argument(
    "store",
    nargs=-1,
    type=click.Choice(models.STORE_DIR),
)
def init_datalad(
    releasedir: Path,
    store: tuple[models.store, ...],
    n_jobs: int = 1,
) -> None:
    init_datalad_wf.main(inroot=releasedir, store=store, n_jobs=n_jobs)


@main.command()
@click.argument(
    "releasedir",
    type=click.Path(
        exists=False, file_okay=False, resolve_path=True, path_type=Path
    ),
)
@click.argument(
    "ria", type=click.Path(exists=False, file_okay=False, path_type=str)
)
@click.argument(
    "store",
    nargs=-1,
    type=click.Choice(models.STORE_DIR),
)
def add_ria(
    releasedir: Path,
    ria: str,
    store: tuple[models.store, ...],
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
    add_ria_wf.main(releasedir=releasedir, ria=ria, store=store)


@main.command()
@click.argument(
    "releasedir",
    type=click.Path(
        exists=False, file_okay=False, resolve_path=True, path_type=Path
    ),
)
@click.argument(
    "ria", type=click.Path(exists=True, file_okay=False, path_type=Path)
)
@click.argument(
    "store",
    nargs=-1,
    type=click.Choice(models.STORE_DIR),
)
@click.option("--n-jobs", type=click.IntRange(min=1, min_open=True), default=1)
def archive(
    releasedir: Path,
    ria: Path,
    store: tuple[models.store, ...],
    n_jobs: int = 1,
) -> None:
    archive_wf.main(releasedir=releasedir, ria=ria, store=store, n_jobs=n_jobs)

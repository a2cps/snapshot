from pathlib import Path
from typing import Literal

import click

from snapshot import flows


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
@click.option("--max-subs", type=int, default=float("inf"))
def stage(
    inroot: Path, outroot: Path, max_subs: float | int = float("inf")
) -> None:
    flows.stage.main(inroot=inroot, outroot=outroot, max_subs=max_subs)


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
    flows.copy_v1_to_dst.main(inroot=inroot, outroot=outroot)


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
    type=click.Choice(
        (
            "rawdata",
            "cat12",
            "fmriprep-anat",
            "fmriprep-cuff",
            "fmriprep-rest",
            "qsiprep",
            "mriqc",
            "freesurfer",
        )
    ),
)
def init_datalad(
    releasedir: Path,
    store: tuple[
        Literal[
            "rawdata",
            "cat12",
            "qsiprep",
            "mriqc",
            "fmriprep-anat",
            "fmriprep-cuff",
            "fmriprep-rest",
            "freesurfer",
        ],
        ...,
    ],
    n_jobs: int = 1,
) -> None:
    flows.init_datalad.main(releasedir=releasedir, store=store, n_jobs=n_jobs)


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
    type=click.Choice(
        (
            "rawdata",
            "cat12",
            "fmriprep-anat",
            "fmriprep-cuff",
            "fmriprep-rest",
            "qsiprep",
            "mriqc",
            "freesurfer",
        )
    ),
)
def add_ria(
    releasedir: Path,
    ria: str,
    store: tuple[
        Literal[
            "rawdata",
            "cat12",
            "qsiprep",
            "mriqc",
            "fmriprep-anat",
            "fmriprep-cuff",
            "fmriprep-rest",
            "freesurfer",
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
    flows.add_ria.main(releasedir=releasedir, ria=ria, store=store)


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
    type=click.Choice(
        (
            "rawdata",
            "cat12",
            "fmriprep-anat",
            "fmriprep-cuff",
            "fmriprep-rest",
            "qsiprep",
            "mriqc",
            "freesurfer",
        )
    ),
)
@click.option("--n-jobs", type=click.IntRange(min=1, min_open=True), default=1)
def archive(
    releasedir: Path,
    ria: Path,
    store: tuple[
        Literal[
            "rawdata",
            "cat12",
            "qsiprep",
            "mriqc",
            "fmriprep-anat",
            "fmriprep-cuff",
            "fmriprep-rest",
            "freesurfer",
        ],
        ...,
    ],
    n_jobs: int = 1,
) -> None:
    flows.archive.main(
        releasedir=releasedir, ria=ria, store=store, n_jobs=n_jobs
    )

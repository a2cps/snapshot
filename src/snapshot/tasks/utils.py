import logging
import re
import shutil
from pathlib import Path

import datalad.api as dla

GITATTRIBUTES = """
* annex.backend=MD5E
* annex.largefiles=(((mimeencoding=binary)and(largerthan=0))or((include=*gii)or(include=*svg)or(include=*html)))
"""


def init_and_save(dataset: Path, n_jobs: int = 1) -> None:
    # initialize the repository
    dla.create(path=dataset, force=True)  # type: ignore
    (dataset / ".gitattributes").write_text(GITATTRIBUTES)

    # store files in repo
    dla.save(dataset=dataset, jobs=n_jobs)  # type: ignore


def update(dataset: Path, n_jobs: int = 1) -> None:
    dla.save(dataset=dataset, jobs=n_jobs)  # type: ignore


def add_ria(dataset: Path, alias: str, ria: str, ria_sibling: str = "ria") -> None:
    # create RIA
    dla.create_sibling_ria(  # type: ignore
        url=ria,
        name=ria_sibling,
        dataset=dataset,
        alias=alias,
        new_store_ok=True,
    )


def archive_to_ria2(dataset: Path, archive_dir: Path, ria_sibling: str = "ria", n_jobs: int = 1) -> None:
    # https://neurostars.org/t/what-is-the-preferred-strategy-for-creating-and-updating-an-archive-7z-in-a-ria-store/25683/5

    # push to RIA
    dla.push(dataset=dataset, to=ria_sibling, jobs=n_jobs)  # type: ignore

    # export archive (always calls 7z with update flag)
    dla.export_archive_ora(dataset=dataset, target=archive_dir)  # type: ignore


def _get_entity(f: Path, pattern: str) -> str:
    possibility = re.findall(pattern, str(f))
    if not len(possibility):
        raise ValueError
    return possibility[0]


def _get_sub(f: Path) -> str:
    return _get_entity(f=f, pattern=r"(?<=sub-)\d+")


def _get_ses(f: Path) -> str:
    return _get_entity(f=f, pattern=r"V[13]")


def _copy_if_needed(src, dst, *args, **kwargs) -> Path:
    if not Path(dst).exists():
        return shutil.copy2(src, dst, *args, **kwargs)

    logging.info(f"File {src} would overwrite {dst}. Leaving files unchanged.")
    return dst

import re
from pathlib import Path

import datalad.api as dla

GITATTRIBUTES = """
* annex.backend=MD5E
* annex.largefiles=(((mimeencoding=binary)and(largerthan=0))or((include=*gii)or(include=*svg)or(include=*html)))
"""


def create_save_and_ria(
    dataset: Path, alias: str, n_jobs: int = 1, ria: str | None = None, ria_sibling: str = "ria"
) -> None:
    # initialize the repository
    dla.create(path=dataset, force=True)  # type: ignore
    (dataset / ".gitattributes").write_text(GITATTRIBUTES)

    # store files in repo
    dla.save(dataset=dataset, jobs=n_jobs)  # type: ignore

    if ria:
        # create RIA
        dla.create_sibling_ria(  # type: ignore
            url=ria,
            name=ria_sibling,
            dataset=dataset,
            alias=alias,
            new_store_ok=True,
        )

        # push to RIA
        dla.push(dataset=dataset, to=ria_sibling, jobs=n_jobs)  # type: ignore


def _get_entity(f: Path, pattern: str) -> str:
    possibility = re.findall(pattern, str(f))
    if not len(possibility):
        raise ValueError
    return possibility[0]


def _get_sub(f: Path) -> str:
    return _get_entity(f=f, pattern=r"(?<=sub-)\d+")


def _get_ses(f: Path) -> str:
    return _get_entity(f=f, pattern=r"V[13]")

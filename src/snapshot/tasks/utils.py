import logging
import re
import shutil
from pathlib import Path

import datalad.api as dla
import nibabel as nb
import numpy as np
from nilearn import masking

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


def add_ria(
    dataset: Path, alias: str, ria: str, ria_sibling: str = "ria"
) -> None:
    # create RIA
    dla.create_sibling_ria(  # type: ignore
        url=ria,
        name=ria_sibling,
        dataset=dataset,
        alias=alias,
        new_store_ok=True,
    )


def archive_to_ria2(
    dataset: Path, archive_dir: Path, ria_sibling: str = "ria", n_jobs: int = 1
) -> None:
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
    return _get_entity(f=f, pattern=r"\d{5}")


def _get_ses(f: Path) -> str:
    return _get_entity(f=f, pattern=r"V[13]")


def _copy_if_needed(src, dst, *args, **kwargs) -> Path:  # noqa: ARG001
    if Path(dst).exists():
        logging.info(
            f"File {src} would overwrite {dst}. Leaving files unchanged."
        )
    elif (src2 := Path(src)).is_symlink():
        Path(dst).symlink_to(Path(src2).resolve())
    else:
        # otherwise, copy the file
        shutil.copy2(src, dst)
    return dst


def _symlink_if_needed(src, dst, *args, **kwargs) -> Path:  # noqa: ARG001
    if Path(dst).exists():
        logging.info(
            f"File {src} would overwrite {dst}. Leaving files unchanged."
        )
    else:
        Path(dst).symlink_to(Path(src).resolve())
    return dst


def _deface(volume: Path, mask: Path, make_mask: bool = False) -> None:  # type: ignore  # noqa: FBT002, FBT001
    if make_mask:
        _mask = nb.load(mask)  # type: ignore
        mask_data = np.asarray(_mask.get_fdata() > 0, dtype=np.uint8)
        mask: nb.Nifti1Image = nb.Nifti1Image(mask_data, affine=_mask.affine)  # type: ignore

    masked_data = masking.apply_mask(volume, mask)
    masked: nb.Nifti1Image = masking.unmask(masked_data, mask)  # type: ignore
    volume.unlink()
    nb.save(masked, volume)  # type: ignore

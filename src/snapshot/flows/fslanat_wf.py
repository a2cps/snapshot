import shutil
from pathlib import Path

from snapshot.tasks import utils


def main(outdir: Path, inroot: Path) -> None:
    if not outdir.exists():
        outdir.mkdir(parents=True)

    for src in inroot.glob("fslanat/*"):
        shutil.copytree(
            src,
            outdir,
            dirs_exist_ok=True,
            copy_function=utils._copy_if_needed,
        )

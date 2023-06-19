import shutil
from pathlib import Path

from snapshot import models
from snapshot.tasks import utils


def main(
    releasedir: Path,
    store: tuple[models.store, ...],
    ria: Path,
    n_jobs: int = 1,
):
    for s in store:
        ria_dir = ria / "alias" / s
        utils.archive_to_ria2(
            dataset=releasedir / s,
            archive_dir=ria_dir / "archives" / "archive.7z",
            n_jobs=n_jobs,
        )
        shutil.rmtree(ria_dir / "annex" / "objects")

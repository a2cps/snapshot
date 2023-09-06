import shutil
from pathlib import Path

from snapshot import models
from snapshot.tasks import utils


def main(releasedir: Path, ria: Path, n_jobs: int = 1):
    for s in models.STORE_DIR:
        match s:
            case "bids":
                dataset = releasedir / s
            case _:
                dataset = releasedir / "derivatives" / s
        ria_dir = ria / "alias" / s
        utils.archive_to_ria2(
            dataset=dataset,
            archive_dir=ria_dir / "archives" / "archive.7z",
            n_jobs=n_jobs,
        )
        shutil.rmtree(ria_dir / "annex" / "objects")

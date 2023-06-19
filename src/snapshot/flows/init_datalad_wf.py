from pathlib import Path

from snapshot import models
from snapshot.tasks import utils


def main(
    inroot: Path,
    store: tuple[models.store, ...],
    n_jobs: int = 1,
) -> None:
    for s in store:
        utils.init_and_save(inroot / s, n_jobs=n_jobs)

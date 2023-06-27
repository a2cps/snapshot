from pathlib import Path

from snapshot import models
from snapshot.tasks import utils


def main(inroot: Path, n_jobs: int = 1) -> None:
    for s in models.STORE_DIR:
        match s:
            case "bids":
                injobdir = inroot / s
            case _:
                injobdir = inroot / "derivatives" / s
        utils.init_and_save(injobdir, n_jobs=n_jobs)

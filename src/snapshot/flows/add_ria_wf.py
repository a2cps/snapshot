from pathlib import Path

from snapshot import models
from snapshot.tasks import utils


def main(
    releasedir: Path,
    store: tuple[models.store, ...],
    ria: str,
):
    for s in store:
        utils.add_ria(dataset=releasedir / s, alias=s, ria=ria)

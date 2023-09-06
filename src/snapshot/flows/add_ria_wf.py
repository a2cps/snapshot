from pathlib import Path

import datalad.api as dla

from snapshot import models


def main(releasedir: Path, ria: str):
    for s in models.STORE_DIR:
        match s:
            case "bids":
                dataset = releasedir / s
            case _:
                dataset = releasedir / "derivatives" / s
        dla.create_sibling_ria(  # type: ignore
            url=ria,
            name="ria",
            dataset=dataset,
            alias=s,
            new_store_ok=True,
        )

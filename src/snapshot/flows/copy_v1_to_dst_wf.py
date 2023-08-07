import shutil
from pathlib import Path

from snapshot import datasets, models
from snapshot.tasks import utils

# TODO: filter input table

def main(inroot: Path, outroot: Path) -> None:
    records = datasets.get_v1_recordids()
    for job in models.STORE_DIR:
        match job:
            case "bids":
                injobdir = inroot / job
                outjobdir = outroot / job
            case _:
                injobdir = inroot / job
                outjobdir = outroot / "derivatives" / job

        # get list of subjects that are present in the input
        # directory but which won't be included in release
        subs_to_exclude = set()
        for file in injobdir.rglob("sub-*"):
            sub = int(utils._get_sub(file))
            if sub not in records:
                subs_to_exclude.add(sub)

        # copy all files from input directory, except those excluded subs
        # and any V3 data
        shutil.copytree(
            injobdir,
            outjobdir,
            ignore=shutil.ignore_patterns(
                "*V3*", *[f"*{sub}*" for sub in subs_to_exclude]
            ),
        )

    # handle top-level stuff
    records = datasets.get_v1_recordids()
    utils._write_participants(records=records, outdir=outroot / "bids")
    utils._update_scans(outdir=outroot / "bids")
    utils._write_events(outdir=outroot / "bids")

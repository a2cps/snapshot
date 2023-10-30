import asyncio
import os
import shutil
import typing
from concurrent import futures
from pathlib import Path

from snapshot import datasets, models
from snapshot.tasks import utils


def copy_directory(src: str, dst: str) -> None:
    if Path(src).is_dir():
        shutil.copy2(src, dst)


async def copytree(src: Path, dst: Path, ignore: typing.Callable | None=None, max_workers: int | None = None) -> None:
    """Copy a directory tree using multiple threads

    Args:
        src (Path): See copytree
        dst (Path): See copytree
        ignore (typing.Callable): See copytree
        max_workers (int | None, optional): See ThreadPoolExecutor. Defaults to None.

    Details:
        The directories are created in the destination first. This is done synchronously to
        avoid race conditions. After the directory tree has been created in the destination,
        the files can all be copied over in a single gather.
    """
    shutil.copytree(
            src,
            dst,
            ignore=ignore,
            copy_function=copy_directory
        )
    loop = asyncio.get_running_loop()
    with futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        awaitables = []
        for (dirpath, _, filenames) in os.walk(src):
            d = Path(dirpath)
            dirpath_fromtop = d.relative_to(src)
            dirpath_dst = dst / dirpath_fromtop
            if not dirpath_dst.exists():
                continue
            for file in filenames:
                # note that we do not need the follow_symlinks argument to copyfile
                awaitables.append(
                    loop.run_in_executor(
                        pool, shutil.copyfile, d / file, dirpath_dst / file
                    )
                )
        await asyncio.gather(*awaitables)



async def main(inroot: Path, outroot: Path, max_workers: int | None = None) -> None:
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
        for file in injobdir.glob("sub-*"):
            sub = int(utils._get_sub(file))
            if sub not in records:
                subs_to_exclude.add(sub)

            # need to exclude participants that only have ses-V3 outputs available
            # as subdirectories. This matters for cases like fmriprep-anat, with outputs
            # fmriprep-anat/sub-10003/{figures,log,ses-V3}; ses-V3 will be excluded,
            # but we'd still get fmriprep-anat/sub-10003/{figures,log}
            # Names like sub-25052_ses-V3_T1w.anat will be excluded due to V3 in
            # the name at the top level
            if (
                file.is_dir()
                and (len(sessions := list(file.glob("ses*"))) == 1)
                and ("V3" in sessions[0].name)
                ):
                subs_to_exclude.add(sub)

        # copy all files from input directory, except those excluded subs
        # and any V3 data
        await copytree(
            injobdir,
            outjobdir,
            ignore=shutil.ignore_patterns(
                "*V3*", *(f"*{sub}*" for sub in subs_to_exclude)
            ),
            max_workers=max_workers
        )

    # handle top-level stuff
    utils._write_participants(records=records, outdir=outroot / "bids")
    utils._update_scans(outdir=outroot / "bids")
    utils._write_events(outdir=outroot / "bids")
    utils._write_readme(outdir=outroot / "bids")
    utils.write_freesurfer_tables_and_jsons(outroot=outroot, inroot=inroot, records=records)
    utils.write_fslanat_tables_and_jsons(outroot=outroot, inroot=inroot, records=records)
    utils.write_fcn_jsons(outroot=outroot)
    utils.write_signatures_jsons(outroot=outroot)

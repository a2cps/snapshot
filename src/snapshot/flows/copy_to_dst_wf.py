import asyncio
import logging
import os
import shutil
import typing
from concurrent import futures
from pathlib import Path

from snapshot import datasets
from snapshot.models import jobs
from snapshot.tasks import utils


def copy_directory(src: str, dst: str) -> None:
    "Used for making directories in copytree (which has a call to mkdirs)"
    pass


def link(src: Path, dst: Path) -> None:
    if not src.is_symlink():
        dst.symlink_to(src)
    else:
        dst.symlink_to(src.resolve())


async def copytree(
    src: Path,
    dst: Path,
    ignore: typing.Callable | None = None,
    max_workers: int | None = None,
) -> None:
    """Copy a directory tree using multiple threads

    Args:
        src (Path): See copytree
        dst (Path): See copytree
        ignore (typing.Callable): See copytree
        max_workers (int | None, optional): See ThreadPoolExecutor. Defaults to None.

    Details:
        The directories are created in the destination first. This is done synchronously
        to avoid race conditions. After the directory tree has been created in the
        destination, the files can all be linked to in a single gather.
    """
    shutil.copytree(src, dst, ignore=ignore, copy_function=copy_directory)
    with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for dirpath, _, filenames in os.walk(src):
            d = Path(dirpath)
            dirpath_fromtop = d.relative_to(src)
            dirpath_dst = dst / dirpath_fromtop
            if not dirpath_dst.exists():
                continue
            # need to ensure ignore is applied to files, too
            if ignore is not None:
                ignored_names = ignore(os.fspath(src), filenames)
            else:
                ignored_names = ()
            for file in filenames:
                if file in ignored_names:
                    continue
                executor.submit(link, d / file, dirpath_dst / file)


def main(
    inroot: Path,
    outroot: Path,
    max_workers: int | None = None,
    jobs_to_copy: typing.Sequence[jobs.STORE_DIR] = jobs.STORE_DIRS,
) -> None:
    records = datasets.get_recordids()
    for job in jobs_to_copy:
        logging.info(f"Working on {job}")
        injobdir = inroot / job
        match job:
            case "bids":
                outjobdir = outroot / "rawdata"
            case _:
                outjobdir = outroot / "derivatives" / job

        # get list of subjects that are present in the input
        # directory but which won't be included in release
        # this pattern covers:

        subs_to_exclude = set()
        match job:
            case (
                "bedpostx"
                | "bids"
                | "brainager"
                | "cat12"
                | "eddyqc"
                | "fmriprep"
                | "freesurfer"
                | "fslanat"
                | "mriqc"
                | "gift"
                | "qsiprep-V1"
                | "synthstrip"
            ):
                generator = injobdir.glob("sub-*")
            case "dwi_biomarker1":
                generator = injobdir.glob("networks/sub-*")
            case "fcn" | "signatures":
                generator = injobdir.glob("*cleaned/sub-*")
            case "postdtifit":
                generator = injobdir.glob("diffusion_regional/sub-*")
            case "postgift":
                generator = injobdir.glob("amplitude/sub=*")
            case "qsirecon_fsl_dtifit":
                generator = injobdir.glob("qsirecon-fsl/sub-*")
        for file in generator:
            match job:
                case "postgift":
                    sub = int(utils._get_entity(f=file, pattern=r"(?<=sub=)\d{5}"))
                case _:
                    sub = int(utils._get_sub(file))
            if sub not in records:
                subs_to_exclude.add(sub)

            # need to exclude participants that only have ses-V3 outputs available
            # as subdirectories. This matters for cases like fmriprep, with outputs
            # fmriprep/sub-10003/{figures,log,ses-V3}; ses-V3 will be excluded,
            # but we'd still get fmriprep/sub-10003/{figures,log}
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
        logging.info(f"Copying {injobdir}, excluding {subs_to_exclude}")
        asyncio.run(
            copytree(
                injobdir,
                outjobdir,
                ignore=shutil.ignore_patterns(
                    "*V3*", *(f"*{sub}*" for sub in subs_to_exclude)
                ),
                max_workers=max_workers,
            )
        )

    # handle top-level stuff
    # bids
    shutil.copy2(datasets.get_dataset_description_json(), outroot / "rawdata")
    utils.write_participants(records=records, outdir=outroot / "rawdata")
    utils.write_sessions(outdir=outroot / "rawdata")
    utils.update_scans(outdir=outroot / "rawdata")
    utils.write_events(outdir=outroot / "rawdata")
    utils.write_readme(outdir=outroot / "rawdata")
    utils.write_changes(outdir=outroot / "rawdata")
    utils.clean_sidecars(root=outroot / "rawdata")

    # fmriprep
    utils.clean_fmriprep_logs(
        inroot=inroot / "fmriprep", outroot=outroot / "derivatives" / "fmriprep"
    )

    # cat12
    utils.write_cat12_tables_and_jsons(outroot=outroot, inroot=inroot, records=records)

    # dwi_biomarker1
    utils.write_dwi_biomarker1_jsons(outroot=outroot)

    # fcn
    utils.write_fcn_jsons(outroot=outroot)

    # freesurfer
    utils.write_freesurfer_tables_and_jsons(
        outroot=outroot, inroot=inroot, records=records
    )

    # fslanat
    utils.write_fslanat_tables_and_jsons(
        outroot=outroot, inroot=inroot, records=records
    )

    # postdtifit
    utils.write_postdtifit_jsons(outroot=outroot)

    # postgift
    utils.write_postgift_jsons(outroot=outroot)

    # signatures
    utils.write_signatures_jsons(outroot=outroot)

    # idps
    utils.write_idps(inroot=inroot, outroot=outroot)

    # everything
    utils.write_release_notes(outroot=outroot)

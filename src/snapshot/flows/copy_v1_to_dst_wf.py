import shutil
from pathlib import Path

from snapshot import datasets, models
from snapshot.tasks import utils


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
        shutil.copytree(
            injobdir,
            outjobdir,
            ignore=shutil.ignore_patterns(
                "*V3*", *(f"*{sub}*" for sub in subs_to_exclude)
            ),
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

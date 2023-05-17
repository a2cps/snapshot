import os
import shutil
from pathlib import Path
from typing import Literal

# from dask import config
# import prefect_dask

from snapshot.flows import cat12, fmriprep, freesurfer, mriqc, qsiprep, rawdata, deface
from snapshot.tasks import utils

SITE_LONG = {
    "NS": "NS_northshore",
    "UI": "UI_uic",
    "UC": "UC_uchicago",
    "UM": "UM_umichigan",
    "SH": "SH_spectrum_health",
    "WS": "WS_wayne_state",
}


def stage(
    inroot: Path,
    outroot: Path,
    deface_root: Path,
    n_workers: int = 1,
):
    # Recursively create symlinks in the target directory
    for site_long in SITE_LONG.values():
        for job in ["bids", "fmriprep", "cat12", "mriqc", "qsiprep"]:
            in_job_dir = inroot / site_long / job
            out_job_dir = outroot / site_long / job
            if not out_job_dir.exists():
                out_job_dir.mkdir(parents=True)

            for root, dirs, files in os.walk(in_job_dir, followlinks=True):
                root_path = Path(root)
                if "work" in root_path.parts:
                    continue

                # make directories for next iteration of walk
                for dir in dirs:
                    out_d = out_job_dir / (root_path / dir).relative_to(in_job_dir)
                    out_d.mkdir()

                # make symlinks for all available files
                for file in files:
                    (out_job_dir / root_path.relative_to(in_job_dir) / file).symlink_to(root_path / file)

    to_deface = [x for x in outroot.rglob("*T1w.nii.gz")]
    deface_qa = [deface_root / x.relative_to(outroot) for x in to_deface]

    # config.set({"distributed.worker.memory.rebalance.measure": "managed_in_memory"})
    # config.set({"distributed.worker.memory.spill": False})
    # config.set({"distributed.worker.memory.target": False})
    # config.set({"distributed.worker.memory.pause": False})
    # config.set({"distributed.worker.memory.terminate": False})
    # config.set({"distributed.comm.timeouts.connect": "90s"})
    # config.set({"distributed.comm.timeouts.tcp": "90s"})
    # config.set({"distributed.nanny.pre-spawn-environ.MALLOC_TRIM_THRESHOLD_": 0})

    deface.deface(volume=to_deface, out=to_deface, odir=deface_qa)

    # deface.deface.with_options(
    #     task_runner=prefect_dask.DaskTaskRunner(
    #         cluster_kwargs={
    #             "n_workers": n_workers,
    #             "threads_per_worker": 1,
    #             "dashboard_address": None,
    #         }
    #     )
    # )(volume=to_deface, out=to_deface, odir=deface_qa)


def init(
    inroot: Path,
    outdir: Path,
    store: tuple[
        Literal[
            "rawdata", "cat12", "qsiprep", "mriqc", "fmriprep-anat", "fmriprep-cuff", "fmriprep-rest", "freesurfer"
        ],
        ...,
    ],
    n_jobs: int = 1,
):
    action = "init"
    if "rawdata" in store:
        rawdata.main(inroot=inroot, outdir=outdir / "rawdata", n_jobs=n_jobs, action=action)
    if "cat12" in store:
        cat12.main(inroot=inroot, outdir=outdir / "cat12", n_jobs=n_jobs, action=action)
    if "qsiprep" in store:
        qsiprep.main(inroot=inroot, outdir=outdir / "qsiprep", n_jobs=n_jobs, action=action)
    if "mriqc" in store:
        mriqc.main(inroot=inroot, outdir=outdir / "mriqc", n_jobs=n_jobs, action=action)
    if "fmriprep-anat" in store:
        fmriprep.main(inroot=inroot, outdir=outdir / "fmriprep-anat", n_jobs=n_jobs, action=action)
    if "fmriprep-cuff" in store:
        fmriprep.main(inroot=inroot, outdir=outdir / "fmriprep-cuff", n_jobs=n_jobs, action=action)
    if "fmriprep-rest" in store:
        fmriprep.main(inroot=inroot, outdir=outdir / "fmriprep-rest", n_jobs=n_jobs, action=action)
    if "freesurfer" in store:
        freesurfer.main(inroot=inroot, outdir=outdir / "freesurfer", n_jobs=n_jobs, action=action)


def update(
    inroot: Path,
    outdir: Path,
    store: tuple[
        Literal[
            "rawdata", "cat12", "qsiprep", "mriqc", "fmriprep-anat", "fmriprep-cuff", "fmriprep-rest", "freesurfer"
        ],
        ...,
    ],
    n_jobs: int = 1,
):
    action = "update"
    if "rawdata" in store:
        rawdata.main(inroot=inroot, outdir=outdir / "rawdata", n_jobs=n_jobs, action=action)
    if "cat12" in store:
        cat12.main(inroot=inroot, outdir=outdir / "cat12", n_jobs=n_jobs, action=action)
    if "qsiprep" in store:
        qsiprep.main(inroot=inroot, outdir=outdir / "qsiprep", n_jobs=n_jobs, action=action)
    if "mriqc" in store:
        mriqc.main(inroot=inroot, outdir=outdir / "mriqc", n_jobs=n_jobs, action=action)
    if "fmriprep-anat" in store:
        fmriprep.main(inroot=inroot, outdir=outdir / "fmriprep-anat", n_jobs=n_jobs, action=action)
    if "fmriprep-cuff" in store:
        fmriprep.main(inroot=inroot, outdir=outdir / "fmriprep-cuff", n_jobs=n_jobs, action=action)
    if "fmriprep-rest" in store:
        fmriprep.main(inroot=inroot, outdir=outdir / "fmriprep-rest", n_jobs=n_jobs, action=action)
    if "freesurfer" in store:
        freesurfer.main(inroot=inroot, outdir=outdir / "freesurfer", n_jobs=n_jobs, action=action)


def add_ria(
    releasedir: Path,
    store: tuple[
        Literal[
            "rawdata", "cat12", "qsiprep", "mriqc", "fmriprep-anat", "fmriprep-cuff", "fmriprep-rest", "freesurfer"
        ],
        ...,
    ],
    ria: str,
):
    for s in store:
        utils.add_ria(dataset=releasedir / s, alias=s, ria=ria)


def archive(
    releasedir: Path,
    store: tuple[
        Literal[
            "rawdata", "cat12", "qsiprep", "mriqc", "fmriprep-anat", "fmriprep-cuff", "fmriprep-rest", "freesurfer"
        ],
        ...,
    ],
    ria: Path,
    n_jobs: int = 1,
):
    for s in store:
        ria_dir = ria / "alias" / s
        utils.archive_to_ria2(dataset=releasedir / s, archive_dir=ria_dir / "archives" / "archive.7z", n_jobs=n_jobs)
        shutil.rmtree(ria_dir / "annex" / "objects")

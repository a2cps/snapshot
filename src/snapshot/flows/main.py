from pathlib import Path
from typing import Literal

from snapshot.flows import cat12, fmriprep, freesurfer, mriqc, qsiprep, rawdata


def main(
    inroot: Path,
    outdir: Path,
    store: tuple[Literal["rawdata", "cat12", "qsiprep", "mriqc", "fmriprep", "freesurfer"], ...],
    ria: str | None = None,
    n_jobs: int = 1,
):
    if "rawdata" in store:
        rawdata.main(inroot=inroot, outdir=outdir / "rawdata", ria=ria, n_jobs=n_jobs)
    if "cat12" in store:
        cat12.main(inroot=inroot, outdir=outdir / "cat12", ria=ria, n_jobs=n_jobs)
    if "qsiprep" in store:
        qsiprep.main(inroot=inroot, outdir=outdir / "qsiprep", ria=ria, n_jobs=n_jobs)
    if "mriqc" in store:
        mriqc.main(inroot=inroot, outdir=outdir / "mriqc", ria=ria, n_jobs=n_jobs)
    if "fmriprep" in store:
        fmriprep.main(inroot=inroot, outdir=outdir / "fmriprep", ria=ria, n_jobs=n_jobs)
    if "freesurfer" in store:
        freesurfer.main(inroot=inroot, outdir=outdir / "freesurfer", ria=ria, n_jobs=n_jobs)

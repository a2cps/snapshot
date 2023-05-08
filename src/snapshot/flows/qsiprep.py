import json
import shutil
from pathlib import Path
from typing import Any, Literal

from snapshot.tasks import utils

SITE_LONG = {
    "NS": "NS_northshore",
    "UI": "UI_uic",
    "UC": "UC_uchicago",
    "UM": "UM_umichigan",
    "SH": "SH_spectrum_health",
    "WS": "WS_wayne_state",
}

# copied from example participant
DWIQC = {
    "report_type": "dwi_qc_report",
    "pipeline": "qsiprep",
    "pipeline_version": 0,
    "boilerplate": "",
    "metric_explanation": {
        "raw_dimension_x": "Number of x voxels in raw images",
        "raw_dimension_y": "Number of y voxels in raw images",
        "raw_dimension_z": "Number of z voxels in raw images",
        "raw_voxel_size_x": "Voxel size in x direction in raw images",
        "raw_voxel_size_y": "Voxel size in y direction in raw images",
        "raw_voxel_size_z": "Voxel size in z direction in raw images",
        "raw_max_b": "Maximum b-value in s/mm^2 in raw images",
        "raw_neighbor_corr": "Neighboring DWI Correlation (NDC) of raw images",
        "raw_num_bad_slices": "Number of bad slices in raw images (from DSI Studio)",
        "raw_num_directions": "Number of directions sampled in raw images",
        "t1_dimension_x": "Number of x voxels in preprocessed images",
        "t1_dimension_y": "Number of y voxels in preprocessed images",
        "t1_dimension_z": "Number of z voxels in preprocessed images",
        "t1_voxel_size_x": "Voxel size in x direction in preprocessed images",
        "t1_voxel_size_y": "Voxel size in y direction in preprocessed images",
        "t1_voxel_size_z": "Voxel size in z direction in preprocessed images",
        "t1_max_b": "Maximum b-value s/mm^2 in preprocessed images",
        "t1_neighbor_corr": "Neighboring DWI Correlation (NDC) of preprocessed images",
        "t1_num_bad_slices": "Number of bad slices in preprocessed images (from DSI Studio)",
        "t1_num_directions": "Number of directions sampled in preprocessed images",
        "mean_fd": "Mean framewise displacement from head motion",
        "max_fd": "Maximum framewise displacement from head motion",
        "max_rotation": "Maximum rotation from head motion",
        "max_translation": "Maximum translation from head motion",
        "max_rel_rotation": "Maximum rotation relative to the previous head position",
        "max_rel_translation": "Maximum translation relative to the previous head position",
        "t1_dice_distance": "Dice score for the overlap of the T1w-based brain mask and the b=0 ref mask",
    },
    "subjects": [],
}


def _merge_dwiqc(dwiqcs: list[Path]) -> dict[str, Any]:
    subdata = []
    for dwiqc in dwiqcs:
        data: dict[str, Any] = json.loads(dwiqc.read_text())
        possibility = data.get("subjects")
        if not possibility and not isinstance(possibility, list):
            raise AssertionError
        subdata.append(possibility[0])
    out = DWIQC
    out["subjects"] = subdata
    return out


def _store_dwiqcs(dwiqcs: list[Path], outdir: Path) -> None:
    (outdir / "dwiqc.json").write_text(json.dumps(_merge_dwiqc(dwiqcs), indent=2))


def main(outdir: Path, inroot: Path, action: Literal["init", "update"], n_jobs: int = 1) -> None:
    if not outdir.exists():
        outdir.mkdir(parents=True)

    dwiqcs = []
    for site, site_long in SITE_LONG.items():
        for src in inroot.glob(f"{site_long}/qsiprep/{site}*/qsiprep/sub*"):
            sub = utils._get_sub(src)
            ses = utils._get_ses(src)
            dwiqcs.append(src.with_name("dwiqc.json"))
            if src.is_file():
                # assumes that the src is a file like sub-#####.html
                utils._copy_if_needed(src, outdir / f"sub-{sub}_ses-{ses}.html")
            else:
                shutil.copytree(src, outdir / src.name, dirs_exist_ok=True, copy_function=utils._copy_if_needed)

    _store_dwiqcs(dwiqcs, outdir=outdir)

    match action:
        case "init":
            utils.init_and_save(dataset=outdir, n_jobs=n_jobs)
        case "update":
            utils.update(dataset=outdir, n_jobs=n_jobs)

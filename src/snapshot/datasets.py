from importlib import resources
from pathlib import Path

import polars as pl


def get_data(file: str) -> Path:
    with resources.as_file(resources.files("snapshot.data").joinpath(file)) as f:
        out = f
    return out


def get_description(file: str) -> Path:
    with resources.as_file(
        resources.files("snapshot.data.dataset_description").joinpath(file)
    ) as f:
        out = f
    return out


def get_recordids() -> list[int]:
    f = get_data("DataFreeze_3_022825.csv")
    record_ids = pl.read_csv(f).select(pl.col("record_id")).to_series().to_list()
    return record_ids


def get_applied_pressures() -> Path:
    return get_data("applied_pressure.csv")


def get_readme() -> Path:
    return get_data("README.md")


def get_changes() -> Path:
    return get_data("CHANGES")


def get_aparc_json() -> Path:
    return get_data("aparc.json")


def get_aseg_json() -> Path:
    return get_data("aseg.json")


def get_confounds_json() -> Path:
    return get_data("confounds.json")


def get_timeseries_json() -> Path:
    return get_data("timeseries.json")


def get_connectivity_json() -> Path:
    return get_data("connectivity.json")


def get_fslanat_json() -> Path:
    return get_data("fslanat.json")


def get_headers_json() -> Path:
    return get_data("headers.json")


def get_signatures_by_part_json() -> Path:
    return get_data("signatures-by-part.json")


def get_signatures_by_run_json() -> Path:
    return get_data("signatures-by-run.json")


def get_signatures_by_tr_json() -> Path:
    return get_data("signatures-by-tr.json")


def get_signatures_by_part_diff_json() -> Path:
    return get_data("signatures-by-part-diff.json")


def get_signatures_by_run_diff_json() -> Path:
    return get_data("signatures-by-run-diff.json")


def get_signatures_by_tr_diff_json() -> Path:
    return get_data("signatures-by-tr-diff.json")


def get_ilog() -> Path:
    return get_data("imaging-log-20250612T010003Z.csv")


def get_qclog() -> Path:
    return get_data("qc-log-20250612T010003Z.csv")


def get_demographics() -> Path:
    return Path(
        "/corral-secure/projects/A2CPS/products/consortium-data/pre-surgery-release-3-0-0/demographics/reformatted/reformatted_demo.csv"
    )


def get_events_json() -> Path:
    return get_data("task-cuff_events.json")


def get_participants_json() -> Path:
    return get_data("participants.json")


def get_dataset_description_json() -> Path:
    return get_description("raw.json")


def get_scans_json() -> Path:
    return get_data("scans.json")


def get_cat12_json() -> Path:
    return get_data("cluster_volumes.json")


def get_sessions_json() -> Path:
    return get_data("sessions.json")


def get_release_notes() -> Path:
    return get_data("A2CPS_Release_3.0_Notes.docx")


def get_guids() -> Path:
    return get_data("guids.csv")


def get_device_serial_number_file() -> Path:
    return get_data("deviceserialnumber.tsv")


def get_device_serial_number_tbl() -> pl.DataFrame:
    return pl.read_csv(get_device_serial_number_file(), separator="\t")


def get_gm_morph_json() -> Path:
    return get_data("gm_morph.json")


def get_diffusion_regional_stats_json() -> Path:
    return get_data("diffusion_regional_stats.json")


def get_gift_amplitude_json() -> Path:
    return get_data("gift_amplitude.json")


def get_gift_biomarkers_json() -> Path:
    return get_data("gift_biomarkers.json")


def get_gift_connectivity_json() -> Path:
    return get_data("gift_connectivity.json")


def get_mask_volumes_json() -> Path:
    return get_data("mask_volumes.json")


def get_mri_json() -> Path:
    return get_data("mri.json")


def get_disruption_json() -> Path:
    return get_data("disruption.json")


def get_dwi_networks_json() -> Path:
    return get_data("dwi_networks.json")

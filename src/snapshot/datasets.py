from importlib import resources
from pathlib import Path

import polars as pl


def get_v1_recordids() -> list[int]:
    with resources.as_file(
        resources.files("snapshot.data").joinpath("DataFreeze_1_022823.csv")
    ) as f:
        record_ids = pl.read_csv(f).select(pl.col("record_id")).to_series().to_list()
    return record_ids


def get_applied_pressures() -> Path:
    with resources.as_file(
        resources.files("snapshot.data").joinpath("applied_pressure.csv")
    ) as f:
        data_file_path = f
    return data_file_path


def get_readme() -> Path:
    with resources.as_file(resources.files("snapshot.data").joinpath("README.md")) as f:
        data_file_path = f
    return data_file_path


def get_aparc_json() -> Path:
    with resources.as_file(
        resources.files("snapshot.data").joinpath("aparc.json")
    ) as f:
        data_file_path = f
    return data_file_path


def get_aseg_json() -> Path:
    with resources.as_file(resources.files("snapshot.data").joinpath("aseg.json")) as f:
        data_file_path = f
    return data_file_path


def get_connectivity_acompcor_json() -> Path:
    with resources.as_file(
        resources.files("snapshot.data").joinpath("connectivity-acompcor.json")
    ) as f:
        data_file_path = f
    return data_file_path


def get_connectivity_confounds_json() -> Path:
    with resources.as_file(
        resources.files("snapshot.data").joinpath("connectivity-confounds.json")
    ) as f:
        data_file_path = f
    return data_file_path


def get_connectivity_json() -> Path:
    with resources.as_file(
        resources.files("snapshot.data").joinpath("connectivity.json")
    ) as f:
        data_file_path = f
    return data_file_path


def get_fslanat_json() -> Path:
    with resources.as_file(
        resources.files("snapshot.data").joinpath("fslanat.json")
    ) as f:
        data_file_path = f
    return data_file_path


def get_headers_json() -> Path:
    with resources.as_file(
        resources.files("snapshot.data").joinpath("headers.json")
    ) as f:
        data_file_path = f
    return data_file_path


def get_signature_bold_json() -> Path:
    with resources.as_file(
        resources.files("snapshot.data").joinpath("signature-bold.json")
    ) as f:
        data_file_path = f
    return data_file_path


def get_signature_by_part_json() -> Path:
    with resources.as_file(
        resources.files("snapshot.data").joinpath("signature-by-part.json")
    ) as f:
        data_file_path = f
    return data_file_path


def get_signature_by_run_json() -> Path:
    with resources.as_file(
        resources.files("snapshot.data").joinpath("signature-by-run.json")
    ) as f:
        data_file_path = f
    return data_file_path


def get_signature_by_tr_json() -> Path:
    with resources.as_file(
        resources.files("snapshot.data").joinpath("signature-by-tr.json")
    ) as f:
        data_file_path = f
    return data_file_path


def get_signature_confounds_json() -> Path:
    with resources.as_file(
        resources.files("snapshot.data").joinpath("signature-confounds.json")
    ) as f:
        data_file_path = f
    return data_file_path


def get_signature_labels_json() -> Path:
    with resources.as_file(
        resources.files("snapshot.data").joinpath("signature-labels.json")
    ) as f:
        data_file_path = f
    return data_file_path


def get_signature_rawdata_json() -> Path:
    with resources.as_file(
        resources.files("snapshot.data").joinpath("signature-rawdata.json")
    ) as f:
        data_file_path = f
    return data_file_path


def get_ilog() -> Path:
    with resources.as_file(
        resources.files("snapshot.data").joinpath("imaging-log-20240618T010003Z.csv")
    ) as f:
        data_file_path = f
    return data_file_path


def get_qclog() -> Path:
    with resources.as_file(
        resources.files("snapshot.data").joinpath("qc-log-20240618T010003Z.csv")
    ) as f:
        data_file_path = f
    return data_file_path


def get_demographics() -> Path:
    with resources.as_file(
        resources.files("snapshot.data").joinpath("demographics-2023-10-31.csv")
    ) as f:
        data_file_path = f
    return data_file_path


def get_events_json() -> Path:
    with resources.as_file(
        resources.files("snapshot.data").joinpath("task-cuff_events.json")
    ) as f:
        data_file_path = f
    return data_file_path


def get_participants_json() -> Path:
    with resources.as_file(
        resources.files("snapshot.data").joinpath("participants.json")
    ) as f:
        data_file_path = f
    return data_file_path


def get_dataset_description_json() -> Path:
    with resources.as_file(
        resources.files("snapshot.data").joinpath("dataset_description.json")
    ) as f:
        data_file_path = f
    return data_file_path


def get_scans_json() -> Path:
    with resources.as_file(
        resources.files("snapshot.data").joinpath("scans.json")
    ) as f:
        data_file_path = f
    return data_file_path


def get_cat12_json() -> Path:
    with resources.as_file(
        resources.files("snapshot.data").joinpath("cluster_volumes.json")
    ) as f:
        data_file_path = f
    return data_file_path


def get_sessions_json() -> Path:
    with resources.as_file(
        resources.files("snapshot.data").joinpath("sessions.json")
    ) as f:
        data_file_path = f
    return data_file_path


def get_release_notes(version: str) -> Path:
    with resources.as_file(
        resources.files("snapshot.data").joinpath(f"A2CPS_Release_{version}_Notes.docx")
    ) as f:
        data_file_path = f
    return data_file_path

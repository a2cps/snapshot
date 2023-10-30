from importlib import resources
from pathlib import Path

import pandas as pd


def get_v1_recordids() -> list[int]:
    with resources.path("snapshot.data", "DataFreeze_1_022823.csv") as f:
        record_ids = pd.read_csv(f).record_id.to_list()
    return record_ids


def get_applied_pressures() -> Path:
    with resources.path("snapshot.data", "applied_pressure.csv") as f:
        data_file_path = f
    return data_file_path


def get_readme() -> Path:
    with resources.path("snapshot.data", "README") as f:
        data_file_path = f
    return data_file_path


def get_aparc_json() -> Path:
    with resources.path("snapshot.data", "aparc.json") as f:
        data_file_path = f
    return data_file_path


def get_aseg_json() -> Path:
    with resources.path("snapshot.data", "aseg.json") as f:
        data_file_path = f
    return data_file_path


def get_connectivity_acompcor_json() -> Path:
    with resources.path("snapshot.data", "connectivity-acompcor.json") as f:
        data_file_path = f
    return data_file_path


def get_connectivity_confounds_json() -> Path:
    with resources.path("snapshot.data", "connectivity-confounds.json") as f:
        data_file_path = f
    return data_file_path


def get_connectivity_json() -> Path:
    with resources.path("snapshot.data", "connectivity.json") as f:
        data_file_path = f
    return data_file_path


def get_fslanat_json() -> Path:
    with resources.path("snapshot.data", "fslanat.json") as f:
        data_file_path = f
    return data_file_path


def get_headers_json() -> Path:
    with resources.path("snapshot.data", "headers.json") as f:
        data_file_path = f
    return data_file_path


def get_signature_bold_json() -> Path:
    with resources.path("snapshot.data", "signature-bold.json") as f:
        data_file_path = f
    return data_file_path


def get_signature_by_part_json() -> Path:
    with resources.path("snapshot.data", "signature-by-part.json") as f:
        data_file_path = f
    return data_file_path


def get_signature_by_run_json() -> Path:
    with resources.path("snapshot.data", "signature-by-run.json") as f:
        data_file_path = f
    return data_file_path


def get_signature_by_tr_json() -> Path:
    with resources.path("snapshot.data", "signature-by-tr.json") as f:
        data_file_path = f
    return data_file_path


def get_signature_confounds_json() -> Path:
    with resources.path("snapshot.data", "signature-confounds.json") as f:
        data_file_path = f
    return data_file_path


def get_signature_labels_json() -> Path:
    with resources.path("snapshot.data", "signature-labels.json") as f:
        data_file_path = f
    return data_file_path


def get_signature_rawdata_json() -> Path:
    with resources.path("snapshot.data", "signature-rawdata.json") as f:
        data_file_path = f
    return data_file_path


def get_ilog() -> Path:
    with resources.path(
        "snapshot.data", "imaging-log-20230927T010002Z.csv"
    ) as f:
        data_file_path = f
    return data_file_path


def get_qclog() -> Path:
    with resources.path("snapshot.data", "qc-log-20230927T010002Z.csv") as f:
        data_file_path = f
    return data_file_path


def get_demographics() -> Path:
    with resources.path("snapshot.data", "demographics-2023-05-19.csv") as f:
        data_file_path = f
    return data_file_path


def get_events_json() -> Path:
    with resources.path("snapshot.data", "task-cuff_events.json") as f:
        data_file_path = f
    return data_file_path


def get_participants_json() -> Path:
    with resources.path("snapshot.data", "participants.json") as f:
        data_file_path = f
    return data_file_path

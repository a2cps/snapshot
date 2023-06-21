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

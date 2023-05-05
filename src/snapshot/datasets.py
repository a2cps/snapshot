from importlib import resources
from pathlib import Path


def get_aseg() -> Path:
    with resources.path("release.data", "desc-aseg_dseg.tsv") as f:
        data_file_path = f
    return data_file_path


def get_aparcaseg() -> Path:
    with resources.path("release.data", "desc-aparcaseg_dseg.tsv") as f:
        data_file_path = f
    return data_file_path

import polars as pl

from snapshot import datasets


def test_get_v1_recordids():
    assert isinstance(datasets.get_recordids(), list)


def test_get_applied_pressures():
    assert datasets.get_applied_pressures().exists()


def test_get_readme():
    assert datasets.get_readme().exists()


def test_get_aparc_json():
    assert datasets.get_aparc_json().exists()


def test_get_aseg_json():
    assert datasets.get_aseg_json().exists()


def test_get_timeseries_json():
    assert datasets.get_timeseries_json().exists()


def test_get_confounds_json():
    assert datasets.get_confounds_json().exists()


def test_get_connectivity_json():
    assert datasets.get_connectivity_json().exists()


def test_get_fslanat_json():
    assert datasets.get_fslanat_json().exists()


def test_get_headers_json():
    assert datasets.get_headers_json().exists()


def test_get_signature_by_part_json():
    assert datasets.get_signatures_by_part_json().exists()


def test_get_signature_by_run_json():
    assert datasets.get_signatures_by_run_json().exists()


def test_get_signature_by_tr_json():
    assert datasets.get_signatures_by_tr_json().exists()


def test_get_release_notes():
    assert datasets.get_release_notes("1.1").exists()


def test_get_deviceserialnumber():
    assert isinstance(datasets.get_device_serial_number_tbl(), pl.DataFrame)

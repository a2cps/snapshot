from snapshot import datasets


def test_get_v1_recordids():
    assert isinstance(datasets.get_v1_recordids(), list)


def test_get_applied_pressures():
    assert datasets.get_applied_pressures().exists()


def test_get_readme():
    assert datasets.get_readme().exists()


def test_get_aparc_json():
    assert datasets.get_aparc_json().exists()


def test_get_aseg_json():
    assert datasets.get_aseg_json().exists()


def test_get_connectivity_acompcor_json():
    assert datasets.get_connectivity_acompcor_json().exists()


def test_get_connectivity_confounds_json():
    assert datasets.get_connectivity_confounds_json().exists()


def test_get_connectivity_json():
    assert datasets.get_connectivity_json().exists()


def test_get_fslanat_json():
    assert datasets.get_fslanat_json().exists()


def test_get_headers_json():
    assert datasets.get_headers_json().exists()


def test_get_signature_bold_json():
    assert datasets.get_signature_bold_json().exists()


def test_get_signature_by_part_json():
    assert datasets.get_signature_by_part_json().exists()


def test_get_signature_by_run_json():
    assert datasets.get_signature_by_run_json().exists()


def test_get_signature_by_tr_json():
    assert datasets.get_signature_by_tr_json().exists()


def test_get_signature_confounds_json():
    assert datasets.get_signature_confounds_json().exists()


def test_get_signature_labels_json():
    assert datasets.get_signature_labels_json().exists()


def test_get_signature_rawdata_json():
    assert datasets.get_signature_rawdata_json().exists()

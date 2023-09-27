from pathlib import Path

from snapshot.tasks import utils


def test_write_fcn_jsons(tmp_path: Path) -> None:
    utils.write_fcn_jsons(tmp_path)
    acompcor = (tmp_path / "acompcor.json").exists()
    confounds = (tmp_path / "connectivity-confounds.json").exists()
    connectivity = (tmp_path / "connectivity.json").exists()

    assert all([acompcor, confounds, connectivity])


def test_write_signatures_jsons(tmp_path: Path) -> None:
    utils.write_signatures_jsons(tmp_path)
    part = (tmp_path / "signature-by-part.json").exists()
    run = (tmp_path / "signature-by-run.json").exists()
    tr = (tmp_path / "signature-by-tr.json").exists()
    confounds = (tmp_path / "signature-confounds.json").exists()
    labels = (tmp_path / "signature-labels.json").exists()
    rawdata = (tmp_path / "signature-rawdata.json").exists()

    assert all([part, run, tr, confounds, labels, rawdata])

from pathlib import Path

from snapshot.tasks import utils


def test_write_fcn_jsons(tmp_path: Path) -> None:
    (tmp_path / "derivatives" / "fcn").mkdir(parents=True)
    utils.write_fcn_jsons(tmp_path)
    acompcor = (tmp_path / "derivatives" / "fcn" / "acompcor.json").exists()
    confounds = (
        tmp_path / "derivatives" / "fcn" / "connectivity-confounds.json"
    ).exists()
    connectivity = (
        tmp_path / "derivatives" / "fcn" / "connectivity.json"
    ).exists()

    assert all([acompcor, confounds, connectivity])


def test_write_signatures_jsons(tmp_path: Path) -> None:
    (tmp_path / "derivatives" / "signatures").mkdir(parents=True)
    utils.write_signatures_jsons(tmp_path)
    part = (
        tmp_path / "derivatives" / "signatures" / "signature-by-part.json"
    ).exists()
    run = (
        tmp_path / "derivatives" / "signatures" / "signature-by-run.json"
    ).exists()
    tr = (
        tmp_path / "derivatives" / "signatures" / "signature-by-tr.json"
    ).exists()
    confounds = (
        tmp_path / "derivatives" / "signatures" / "signature-confounds.json"
    ).exists()
    labels = (
        tmp_path / "derivatives" / "signatures" / "signature-labels.json"
    ).exists()
    rawdata = (
        tmp_path / "derivatives" / "signatures" / "signature-rawdata.json"
    ).exists()

    assert all([part, run, tr, confounds, labels, rawdata])

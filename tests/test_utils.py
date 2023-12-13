import json
import typing
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


def test_clean_sidecars(tmp_path: Path) -> None:
    data_with_institution_name = {
        "InstitutionAddress": "East Beltline NE 2750,Grand Rapids,MI,US,49503",
        "InstitutionName": "Spectrum ICCB Campus",
        "SeriesDescription": "T1_MPRAGE_ND",
    }
    data_without_institution_name = {
        "InstitutionName": "Spectrum ICCB Campus",
        "SeriesDescription": "T1_MPRAGE_ND",
    }

    file_with_institution_name = tmp_path / "sub-00_T1w.json"
    file_without_institution_name = tmp_path / "sub-01_T1w.json"
    file_with_institution_name.write_text(
        json.dumps(data_with_institution_name)
    )
    file_without_institution_name.write_text(
        json.dumps(data_without_institution_name)
    )

    utils.clean_sidecars(tmp_path)

    result_from_with: dict[str, typing.Any] = json.loads(
        file_with_institution_name.read_text()
    )
    result_from_without: dict[str, typing.Any] = json.loads(
        file_without_institution_name.read_text()
    )
    no_institutionname = [
        f.get("InstitutionName") is None
        for f in [result_from_with, result_from_without]
    ]
    no_institutionname = [
        f.get("InstitutionAddress") is None
        for f in [result_from_with, result_from_without]
    ]

    assert all(no_institutionname + no_institutionname)

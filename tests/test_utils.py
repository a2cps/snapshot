import json
import typing
from pathlib import Path

from snapshot import datasets
from snapshot.tasks import utils


def test_write_fcn_jsons(tmp_path: Path) -> None:
    (tmp_path / "derivatives" / "fcn").mkdir(parents=True)
    utils.write_fcn_jsons(tmp_path)
    confounds = (tmp_path / "derivatives" / "fcn" / "confounds.json").exists()
    connectivity = (tmp_path / "derivatives" / "fcn" / "connectivity.json").exists()
    timeseries = (tmp_path / "derivatives" / "fcn" / "timeseries.json").exists()

    assert all([confounds, connectivity, timeseries])


def test_write_signatures_jsons(tmp_path: Path) -> None:
    (tmp_path / "derivatives" / "signatures").mkdir(parents=True)
    utils.write_signatures_jsons(tmp_path)
    part = (
        tmp_path / "derivatives" / "signatures" / "signatures-by-part.json"
    ).exists()
    run = (tmp_path / "derivatives" / "signatures" / "signatures-by-run.json").exists()
    tr = (tmp_path / "derivatives" / "signatures" / "signatures-by-tr.json").exists()
    confounds = (tmp_path / "derivatives" / "signatures" / "confounds.json").exists()
    part_diff = (
        tmp_path / "derivatives" / "signatures" / "signatures-by-part-diff.json"
    ).exists()
    run_diff = (
        tmp_path / "derivatives" / "signatures" / "signatures-by-run-diff.json"
    ).exists()
    tr_diff = (
        tmp_path / "derivatives" / "signatures" / "signatures-by-tr-diff.json"
    ).exists()

    assert all([part, run, tr, part_diff, run_diff, tr_diff, confounds])


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
    file_with_institution_name.write_text(json.dumps(data_with_institution_name))
    file_without_institution_name.write_text(json.dumps(data_without_institution_name))

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


def test_write_participants(tmp_path: Path):
    participants = tmp_path / "participants.tsv"
    sidecar = tmp_path / "participants.json"
    utils.write_participants(datasets.get_recordids(), tmp_path)

    assert all([participants.exists(), sidecar.exists()])


def test_write_sessions(tmp_path: Path):
    dst = tmp_path / "sessions.json"
    utils.write_sessions(dst.parent)

    assert dst.exists()


def test_write_readme(tmp_path: Path):
    dst = tmp_path / "README"
    utils.write_readme(dst.parent)

    assert dst.exists()

import json
from pathlib import Path

from snapshot.flows import qsiprep


def test_merge_dwiqc(tmp_path: Path):
    s0 = {
        "report_type": "dwi_qc_report",
        "pipeline": "qsiprep",
        "pipeline_version": 0,
        "boilerplate": "",
        "metric_explanation": {
            "raw_dimension_x": "Number of x voxels in raw images",
        },
        "subjects": [
            {
                "raw_dimension_x": 140.0,
            }
        ],
    }
    dwiqc = tmp_path / "dwiqc.json"
    dwiqc.write_text(json.dumps(s0))
    merged = qsiprep._merge_dwiqc([dwiqc, dwiqc])
    assert merged

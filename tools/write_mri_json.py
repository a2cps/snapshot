import json
from pathlib import Path

dst = Path("../src/snapshot/data/mri.json")


j = json.loads(Path("mri.json").read_text())

for component in range(1, 176):
    j[f"component_{component}_falff"] = {
        "LongName": f"Component {component} fALFF",
        "Description": "Neuromark_fMRI_2.1_modelorder-multi Fractional Amplitude of Low Frequency Fluctuations",
    }

for source in range(1, 176):
    for target in range(1, 176):
        if target > source:
            j[f"source_{source}_target_{target}_connectivity"] = {
                "LongName": f"Connectivity between components {source} and {target}",
                "Description": "Connectivity is a correlation, and the components are from Neuromark_fMRI_2.1_modelorder-multi",
            }


dst.write_text(json.dumps(j, indent=4))

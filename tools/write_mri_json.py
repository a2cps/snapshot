import json
from pathlib import Path

dst = Path("../src/snapshot/data/mri.json")

j = json.loads(Path("mri.json").read_text())

del j["component_#_falff"]
del j["source_#_target_@"]

for task in ["rest", "cuff"]:
    for run in ["1", "2"]:
        for component in range(1, 105):
            j[f"task_{task}_run_{run}_component_{component}_falff"] = {
                "LongName": f"Component {component} fALFF",
                "Description": "Neuromark_fMRI_2.1_modelorder-multi Fractional Amplitude of Low Frequency Fluctuations",
            }

for task in ["rest", "cuff"]:
    for run in ["1", "2"]:
        for source in range(1, 105):
            for target in range(1, 105):
                if target > source:
                    j[
                        f"task_{task}_run_{run}_source_{source}_target_{target}_connectivity"
                    ] = {
                        "LongName": f"Connectivity between components {source} and {target} in {task}{run}",
                        "Description": "Connectivity is a correlation, and the components are from Neuromark_fMRI_2.1_modelorder-multi",
                    }


dst.write_text(json.dumps(j, indent=4))

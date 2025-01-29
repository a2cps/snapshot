# for finding rest2 scans collected with all cuff pressure 0

import polars as pl

pressures = (
    pl.read_csv("../src/snapshot/data/applied_pressure.csv")
    .pivot(on="scan", values="applied_pressure")
    .rename({"CUFF1": "cuff1_pressure", "CUFF2": "cuff2_pressure"})
    .drop("visit")
)

d = (
    pl.read_csv(
        "../src/snapshot/data/imaging-log-20241217T010003Z.csv", null_values=["", "na"]
    )
    .rename(
        {
            "subject_id": "record_id",
            "fMRI Standard Pressure Received": "CUFF2",
            "fMRI Individualized Pressure Received": "CUFF1",
            "2nd Resting State Received": "REST2",
        }
    )
    .filter(pl.col("visit") == "V1")
    .select("record_id", "CUFF1", "CUFF2", "REST2")
    .with_columns(
        pl.col("CUFF1").cast(bool),
        pl.col("CUFF2").cast(bool),
        pl.col("REST2").cast(bool),
    )
    .join(pressures, how="left", on="record_id")
    .filter(
        pl.col("REST2")
        & ~((pl.col("cuff1_pressure") > 0) | (pl.col("cuff2_pressure") > 0))
    )
)

with pl.Config(tbl_rows=20):
    print(d)

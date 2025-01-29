import polars as pl
import requests

# need to rely on qst for contraindicated because imaging log is broken
tka = (
    pl.read_csv("reformatted_tka_qst.csv", null_values=["NA", ""])
    .with_columns(contraindicated=pl.col("fmricuffcontrayn") == 1)
    .select("record_id", "contraindicated")
)
thor = (
    pl.read_csv(
        "reformatted_thor_qst.csv", null_values=["NA", ""], infer_schema_length=10000
    )
    .with_columns(
        contraindicated=pl.when(pl.col("cuffpfmricontraindyn") == 0)
        .then(False)
        .when(
            (pl.col("cuffpfmricontraindyn") == 1)
            & (pl.col("cuffpfmricontrainddomyn") == 1)
        )
        .then(True)
        .otherwise(False)
    )
    .select("record_id", "contraindicated")
)
qst = pl.concat([tka, thor])

data = {
    "token": "",
    "content": "record",
    "action": "export",
    "format": "json",
    "type": "eav",
    "csvDelimiter": "",
    "forms[0]": "flagging_form",
    "events[0]": "baseline_visit_arm_1",
    "rawOrLabel": "raw",
    "rawOrLabelHeaders": "raw",
    "exportCheckboxLabel": "false",
    "exportSurveyFields": "false",
    "exportDataAccessGroups": "false",
    "returnFormat": "json",
}
r = requests.post("https://redcap.tacc.utexas.edu/api/", data=data, timeout=10)


pressures = (
    pl.DataFrame(r.json(), schema_overrides={"record": int})
    .rename({"record": "record_id"})
    .drop("redcap_event_name")
    .pivot(on="field_name", index="record_id")
    .drop("flagging_form_complete")
    .with_columns(
        applied_cuff_pressure=pl.when(
            pl.col("applied_cuff_pressure").str.len_chars() == 0
        )
        .then(None)
        .otherwise(pl.col("applied_cuff_pressure"))
        .cast(int)
    )
    .rename({"applied_cuff_pressure": "applied_pressure"})
)

freeze = pl.read_csv("../src/snapshot/data/DataFreeze_2_022924.csv")

cuff2_0 = (
    pl.read_csv("cuff2_zero.tsv", separator="\t")
    .filter(pl.col("visit") == "V1")
    .select("record_id")
    .to_series()
    .to_list()
)

# need this table because flagging form not yet available outside of MCC1 TKA
cuff1_0 = (
    pl.read_csv("cuff1_zero.tsv", separator="\t")
    .filter(pl.col("visit") == "V1", pl.col("record_id") > 14999)
    .select("record_id")
    .to_series()
    .to_list()
)

ilog_from_form = (
    pl.read_csv(
        "../src/snapshot/data/imaging-log-20241217T010003Z.csv", null_values=["", "na"]
    )
    .rename(
        {
            "subject_id": "record_id",
            "fMRI Standard Pressure Received": "CUFF2",
            "fMRI Individualized Pressure Received": "CUFF1",
            "Cuff1 Applied Pressure": "applied_pressure",
        }
    )
    .filter(pl.col("record_id") > 14999)
    .select("record_id", "visit", "CUFF1", "CUFF2", "applied_pressure")
    .filter(pl.col("visit") == "V1")
    .unpivot(
        index=["record_id", "visit", "applied_pressure"],
        variable_name="scan",
        value_name="indicated",
    )
    .filter(pl.col("indicated") == 1)
    .drop("indicated")
    .with_columns(pl.col("applied_pressure").cast(int))
)

ilog_flagging = (
    pl.read_csv(
        "../src/snapshot/data/imaging-log-20241217T010003Z.csv", null_values=["", "na"]
    )
    .rename(
        {
            "subject_id": "record_id",
            "fMRI Standard Pressure Received": "CUFF2",
            "fMRI Individualized Pressure Received": "CUFF1",
        }
    )
    .filter(pl.col("record_id") <= 14999)
    .select("record_id", "visit", "CUFF1", "CUFF2")
    .filter(pl.col("visit") == "V1")
    .unpivot(
        index=["record_id", "visit"],
        variable_name="scan",
        value_name="indicated",
    )
    .filter(pl.col("indicated") == 1)
    .drop("indicated")
    .join(pressures, on=["record_id"], how="left")
    .select("record_id", "visit", "applied_pressure", "scan")
)

(
    pl.concat([ilog_from_form, ilog_flagging])
    .join(freeze, how="semi", on="record_id")
    .join(qst, how="left", on="record_id")
    .with_columns(
        applied_pressure=pl.when(pl.col("contraindicated") == 1)
        .then(0)
        .otherwise(pl.col("applied_pressure"))
    )
    .with_columns(
        applied_pressure=pl.when(pl.col("scan").str.contains("CUFF2"))
        .then(120)
        .otherwise(pl.col("applied_pressure"))
    )
    .with_columns(
        applied_pressure=pl.when(
            pl.col("scan")
            .str.contains("CUFF1")
            .and_(pl.col("record_id").is_in(cuff1_0))
        )
        .then(0)
        .when(
            pl.col("scan")
            .str.contains("CUFF2")
            .and_(pl.col("record_id").is_in(cuff2_0))
        )
        .then(0)
        .when((pl.col("record_id") == 25068) & (pl.col("scan") == "CUFF1"))
        .then(300)  # difficult redcap case
        .when((pl.col("record_id") == 25171) & (pl.col("scan") == "CUFF1"))
        .then(130)  # difficult redcap case
        .otherwise(pl.col("applied_pressure"))
        .cast(int)
    )
    .select("record_id", "visit", "scan", "applied_pressure")
    .sort("record_id", "visit", "scan")
    .write_csv("../src/snapshot/data/applied_pressure.csv")
)

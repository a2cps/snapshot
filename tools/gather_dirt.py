import polars as pl


def read_dat(src) -> pl.LazyFrame:
    return pl.scan_csv(
        src,
        separator="\t",
        has_header=False,
        null_values=[r"\.", r"\N"],
    ).rename({"column_1": "id"})


# sessions
read_dat("dumpdir/3436.dat.gz").rename(
    {"column_2": "step", "column_3": "created", "column_4": "user"}
).with_columns(
    pl.col("created").str.extract_groups(r"^(.*?)(\.\d+)?([+-]\d{2})$").alias("parts")
).drop("created").with_columns(
    (
        pl.col("parts").struct.field("1")  # Base datetime
        + "."  # Fixed dot
        + pl.col("parts")
        .struct.field("2")
        .str.slice(1)  # Remove extracted dot
        .fill_null("0")  # Fill if no micros
        .str.pad_end(6, "0")  # Pad micros to 6
        + pl.col("parts").struct.field("3")  # The +00 part
        + "00"  # Pad TZ to 4 digits (+0000)
    )
    .str.to_datetime("%Y-%m-%d %H:%M:%S.%6f%z")
    .alias("created")
).drop("parts").sink_parquet("dumpdir/session.parquet")

# images
read_dat("dumpdir/3438.dat.gz").rename(
    {
        "column_2": "img",
        "column_3": "slice",
        "column_4": "file1",
        "column_5": "file2",
        "column_6": "display",
        "column_7": "step",
        "column_8": "created",
    }
).with_columns(pl.col("img").str.slice(3).str.decode("hex")).with_columns(
    pl.col("created").str.extract_groups(r"^(.*?)(\.\d+)?([+-]\d{2})$").alias("parts")
).drop("created").with_columns(
    (
        pl.col("parts").struct.field("1")  # Base datetime
        + "."  # Fixed dot
        + pl.col("parts")
        .struct.field("2")
        .str.slice(1)  # Remove extracted dot
        .fill_null("0")  # Fill if no micros
        .str.pad_end(6, "0")  # Pad micros to 6
        + pl.col("parts").struct.field("3")  # The +00 part
        + "00"  # Pad TZ to 4 digits (+0000)
    )
    .str.to_datetime("%Y-%m-%d %H:%M:%S.%6f%z")
    .alias("created")
).drop("parts").sink_parquet("dumpdir/image.parquet")

read_dat("dumpdir/3440.dat.gz").rename(
    {
        "column_2": "source_data_issue",
        "column_3": "created",
        "column_4": "rating",
        "column_5": "img_id",
        "column_6": "session",
        "column_7": "notes",
    }
).with_columns(
    pl.col("created").str.extract_groups(r"^(.*?)(\.\d+)?([+-]\d{2})$").alias("parts")
).drop("created").with_columns(
    (
        pl.col("parts").struct.field("1")  # Base datetime
        + "."  # Fixed dot
        + pl.col("parts")
        .struct.field("2")
        .str.slice(1)  # Remove extracted dot
        .fill_null("0")  # Fill if no micros
        .str.pad_end(6, "0")  # Pad micros to 6
        + pl.col("parts").struct.field("3")  # The +00 part
        + "00"  # Pad TZ to 4 digits (+0000)
    )
    .str.to_datetime("%Y-%m-%d %H:%M:%S.%6f%z")
    .alias("created")
).drop("parts").with_columns(pl.col("source_data_issue") == pl.lit("t")).sink_parquet(
    "dumpdir/rating.parquet"
)

read_dat("dumpdir/3442.dat.gz").rename(
    {
        "column_2": "source_data_issue",
        "column_3": "created",
        "column_4": "x",
        "column_5": "y",
        "column_6": "img_id",
        "column_7": "session",
        "column_8": "notes",
    }
).with_columns(
    pl.col("created").str.extract_groups(r"^(.*?)(\.\d+)?([+-]\d{2})$").alias("parts")
).drop("created").with_columns(
    (
        pl.col("parts").struct.field("1")  # Base datetime
        + "."  # Fixed dot
        + pl.col("parts")
        .struct.field("2")
        .str.slice(1)  # Remove extracted dot
        .fill_null("0")  # Fill if no micros
        .str.pad_end(6, "0")  # Pad micros to 6
        + pl.col("parts").struct.field("3")  # The +00 part
        + "00"  # Pad TZ to 4 digits (+0000)
    )
    .str.to_datetime("%Y-%m-%d %H:%M:%S.%6f%z")
    .alias("created")
).drop("parts").with_columns(pl.col("source_data_issue") == pl.lit("t")).sink_parquet(
    "dumpdir/coordinate.parquet"
)

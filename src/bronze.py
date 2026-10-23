
"""Incrementally ingest raw CSV files into a Bronze Delta table."""

import re

from pyspark.sql import SparkSession, functions as F

from config import BRONZE_TABLE, LANDING


# Preserve source values as strings.
CSV_OPTIONS = {
    "header": "true",
    "inferSchema": "false",
    "multiLine": "true",
    "quote": '"',
    "escape": '"',
}

METADATA_COLUMNS = {
    "_ingest_timestamp",
    "_source_file",
    "_file_modified_at",
}


def clean_name(name: str) -> str:
    """Make a column name safe for Delta."""
    cleaned = name.replace("%", "pct")
    return re.sub(r"[\s,;{}()=]+", "_", cleaned).strip("_")


def clean_column_names(df):
    """Replace invalid Delta characters in every column name."""
    new_names = [clean_name(c) for c in df.columns]
    if len(set(new_names)) != len(new_names):
        raise ValueError(f"Duplicate column names after cleaning: {new_names}")
    return df.toDF(*new_names)


def create_bronze_table(spark: SparkSession) -> None:
    """Create an empty Delta table using the source CSV schema."""
    if spark.catalog.tableExists(BRONZE_TABLE):
        return
    raw_df = clean_column_names(spark.read.options(**CSV_OPTIONS).csv(LANDING))
    conflicts = METADATA_COLUMNS.intersection(raw_df.columns)
    if conflicts:
        raise ValueError(f"Reserved metadata columns found: {sorted(conflicts)}")
    (raw_df.limit(0)
        .withColumn("_ingest_timestamp", F.lit(None).cast("timestamp"))
        .withColumn("_source_file", F.lit(None).cast("string"))
        .withColumn("_file_modified_at", F.lit(None).cast("timestamp"))
        .write.format("delta").saveAsTable(BRONZE_TABLE))


def load_bronze(spark: SparkSession) -> None:
    """Load new CSV files from Landing using COPY INTO."""

    format_options = ", ".join(
        f"'{key}' = '{value}'"
        for key, value in CSV_OPTIONS.items()
    )

    raw_columns = spark.read.options(**CSV_OPTIONS).csv(LANDING).columns
    select_columns = ", ".join(f"`{c}` AS `{clean_name(c)}`" for c in raw_columns)

    spark.sql(f"""
        COPY INTO {BRONZE_TABLE}
        FROM (
            SELECT
                {select_columns},
                current_timestamp() AS _ingest_timestamp,
                _metadata.file_path AS _source_file,
                _metadata.file_modification_time AS _file_modified_at
            FROM '{LANDING}'
        )
        FILEFORMAT = CSV
        FORMAT_OPTIONS ({format_options})
        COPY_OPTIONS ('mergeSchema' = 'true')
    """)


def rows_per_source_file(spark: SparkSession):
    """Return Bronze row counts grouped by source file."""

    return (
        spark.table(BRONZE_TABLE)
        .groupBy("_source_file")
        .count()
        .orderBy("_source_file")
    )


def reconcile(spark: SparkSession) -> bool:
    """Compare total row counts between Landing and Bronze."""

    landing_rows = (
        spark.read
        .options(**CSV_OPTIONS)
        .csv(LANDING)
        .count()
    )

    bronze_rows = spark.table(BRONZE_TABLE).count()

    matched = landing_rows == bronze_rows

    print(f"Landing rows : {landing_rows}")
    print(f"Bronze rows  : {bronze_rows}")
    print(f"Match        : {matched}")

    return matched


def check_idempotency(spark: SparkSession) -> bool:
    """Check that rerunning the Bronze load does not add duplicate rows."""

    rows_before = spark.table(BRONZE_TABLE).count()

    load_bronze(spark)

    rows_after = spark.table(BRONZE_TABLE).count()

    passed = rows_before == rows_after

    print(f"Rows before reload: {rows_before}")
    print(f"Rows after reload : {rows_after}")
    print(f"Idempotency check : {passed}")

    return passed
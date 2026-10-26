"""Incrementally ingest raw CSV files into a Bronze Delta table."""

import os
import re

from pyspark.sql import SparkSession, functions as F

from config import BRONZE_TABLE, CSV_OPTIONS, LANDING_DIR


METADATA_COLUMNS = {
    "_ingest_timestamp",
    "_source_file",
    "_file_modified_at",
}


def clean_name(name: str) -> str:
    """Normalize a column name for Delta."""
    name = name.strip().replace("%", "pct")
    name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    return re.sub(r"_+", "_", name).strip("_")


def get_landing_files() -> list[str]:
    """Return CSV filenames in Landing."""
    if not os.path.isdir(LANDING_DIR):
        raise FileNotFoundError(
            f"Landing directory not found: {LANDING_DIR}"
        )

    return sorted(
        filename
        for filename in os.listdir(LANDING_DIR)
        if filename.lower().endswith(".csv")
    )



def get_source_columns(spark: SparkSession) -> list[str]:
    """Return normalized source column names."""
    if not get_landing_files():
        raise ValueError("No CSV files found in Landing.")

    df = (
        spark.read
        .options(**CSV_OPTIONS)
        .csv(LANDING_DIR)
    )

    columns = [clean_name(name) for name in df.columns]

    if len(columns) != len(set(columns)):
        raise ValueError("Duplicate column names after cleaning.")

    conflicts = METADATA_COLUMNS.intersection(columns)

    if conflicts:
        raise ValueError(
            f"Reserved metadata columns found: {sorted(conflicts)}"
        )

    return columns


def create_bronze_table(spark: SparkSession) -> None:
    """Create an empty Bronze Delta table if it does not exist."""
    if spark.catalog.tableExists(BRONZE_TABLE):
        return

    source_columns = get_source_columns(spark)

    source_df = (
        spark.read
        .options(**CSV_OPTIONS)
        .csv(LANDING_DIR)
        .limit(0)
        .toDF(*source_columns)
    )

    empty_df = (
        source_df
        .withColumn("_ingest_timestamp", F.lit(None).cast("timestamp"))
        .withColumn("_source_file", F.lit(None).cast("string"))
        .withColumn("_file_modified_at", F.lit(None).cast("timestamp"))
    )

    (
        empty_df.write
        .format("delta")
        .saveAsTable(BRONZE_TABLE)
    )


def load_bronze(spark: SparkSession) -> None:
    """Load new CSV files into Bronze using COPY INTO."""
    if not get_landing_files():
        raise ValueError("No CSV files found in Landing.")

    if not spark.catalog.tableExists(BRONZE_TABLE):
        raise ValueError(
            f"Bronze table does not exist: {BRONZE_TABLE}"
        )

    source_columns = get_source_columns(spark)

    target_columns = [
        column
        for column in spark.table(BRONZE_TABLE).columns
        if column not in METADATA_COLUMNS
    ]

    if source_columns != target_columns:
        raise ValueError(
            "Source columns do not match Bronze table columns. "
            f"Source: {source_columns}; Bronze: {target_columns}"
        )

    raw_columns = (
        spark.read
        .options(**CSV_OPTIONS)
        .csv(LANDING_DIR)
        .columns
    )

    select_columns = ", ".join(
        f"`{raw.replace('`', '``')}` "
        f"AS `{clean_name(raw).replace('`', '``')}`"
        for raw in raw_columns
    )

    format_options = ", ".join(
        f"'{key}' = '{value}'"
        for key, value in CSV_OPTIONS.items()
    )

    spark.sql(f"""
        COPY INTO {BRONZE_TABLE}
        FROM (
            SELECT
                {select_columns},
                current_timestamp() AS _ingest_timestamp,
                _metadata.file_path AS _source_file,
                _metadata.file_modification_time AS _file_modified_at
            FROM '{LANDING_DIR}'
        )
        FILEFORMAT = CSV
        FORMAT_OPTIONS ({format_options})
    """)


def rows_per_source_file(spark: SparkSession):
    """Count Bronze rows by source file."""
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
        .csv(LANDING_DIR)
        .count()
    )

    bronze_rows = spark.table(BRONZE_TABLE).count()
    passed = landing_rows == bronze_rows

    print(f"Landing rows: {landing_rows}")
    print(f"Bronze rows:  {bronze_rows}")
    print(f"Reconciliation passed: {passed}")

    return passed


def check_idempotency(spark: SparkSession) -> bool:
    """Check that reloading the same files adds no rows."""
    rows_before = spark.table(BRONZE_TABLE).count()

    load_bronze(spark)

    rows_after = spark.table(BRONZE_TABLE).count()
    passed = rows_before == rows_after

    print(f"Rows before reload: {rows_before}")
    print(f"Rows after reload:  {rows_after}")
    print(f"Idempotency passed: {passed}")

    return passed

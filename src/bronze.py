"""Incrementally ingest raw CSV files into a Bronze Delta table."""

import os
import re

from pyspark.sql import Column, DataFrame, SparkSession, functions as F

from config import BRONZE_TABLE, CSV_OPTIONS, LANDING_DIR


METADATA_COLUMNS = ["_ingest_timestamp", "_source_file", "_file_modified_at"]


def clean_name(name: str) -> str:
    """Make a column name safe for Delta, e.g. DiscountApplied(%) -> DiscountApplied_pct."""
    name = name.strip().replace("%", "pct")
    return re.sub(r"[^a-zA-Z0-9_]+", "_", name).strip("_")


def file_name_from(column: str) -> Column:
    """Extract the file name from a column that holds a full file path."""
    return F.regexp_extract(F.col(column), r"([^/]+)$", 1)


def get_landing_files() -> list[str]:
    """Return CSV filenames in Landing."""
    if not os.path.isdir(LANDING_DIR):
        raise FileNotFoundError(f"Landing directory not found: {LANDING_DIR}")

    return sorted(
        filename
        for filename in os.listdir(LANDING_DIR)
        if filename.lower().endswith(".csv")
    )


def read_landing(spark: SparkSession) -> DataFrame:
    """Read all CSV files in Landing with every column as a string."""
    if not get_landing_files():
        raise ValueError("No CSV files found in Landing.")

    return spark.read.options(**CSV_OPTIONS).csv(LANDING_DIR)


def create_bronze_table(spark: SparkSession) -> None:
    """Create an empty Bronze Delta table if it does not exist."""
    if spark.catalog.tableExists(BRONZE_TABLE):
        return

    columns = ",\n    ".join(
        f"`{clean_name(name)}` STRING" for name in read_landing(spark).columns
    )

    spark.sql(f"""
        CREATE TABLE {BRONZE_TABLE} (
            {columns},
            _ingest_timestamp TIMESTAMP,
            _source_file STRING,
            _file_modified_at TIMESTAMP
        ) USING DELTA
    """)


def load_bronze(spark: SparkSession) -> None:
    """Load new CSV files into Bronze using COPY INTO."""
    if not spark.catalog.tableExists(BRONZE_TABLE):
        raise ValueError(f"Bronze table does not exist: {BRONZE_TABLE}")

    raw_columns = read_landing(spark).columns
    source_columns = [clean_name(name) for name in raw_columns]

    target_columns = [
        name
        for name in spark.table(BRONZE_TABLE).columns
        if name not in METADATA_COLUMNS
    ]

    if source_columns != target_columns:
        raise ValueError(
            "Source columns do not match Bronze table columns. "
            f"Source: {source_columns}; Bronze: {target_columns}"
        )

    select_columns = ", ".join(
        f"`{raw}` AS `{clean}`" for raw, clean in zip(raw_columns, source_columns)
    )

    format_options = ", ".join(
        f"'{key}' = '{value}'" for key, value in CSV_OPTIONS.items()
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


def rows_per_source_file(spark: SparkSession) -> DataFrame:
    """Count Bronze rows by source file."""
    return (
        spark.table(BRONZE_TABLE)
        .select(file_name_from("_source_file").alias("file_name"))
        .groupBy("file_name")
        .count()
        .orderBy("file_name")
    )



def reconcile(spark: SparkSession) -> bool:
    """Compare Bronze row counts with matching monthly batches in Raw."""
    from config import RAW_DIR, RAW_FILE, DATE_COLUMN

    raw_path = os.path.join(RAW_DIR, RAW_FILE)

    # Read the complete raw dataset.
    raw_df = (
        spark.read
        .options(**CSV_OPTIONS)
        .option("header", True)
        .csv(raw_path)
    )

    # Assign each raw row to the same monthly batch as seed.py.
    raw_df = raw_df.withColumn(
        "_transaction_ts",
        F.try_to_timestamp(
            F.col(DATE_COLUMN),
            F.lit("M/d/yyyy H:mm"),
        ),
    )

    raw_counts = (
        raw_df
        .withColumn(
            "file_name",
            F.when(
                F.col("_transaction_ts").isNull(),
                F.lit("transactions_unknown.csv"),
            ).otherwise(
                F.concat(
                    F.lit("transactions_"),
                    F.date_format(F.col("_transaction_ts"), "yyyy-MM"),
                    F.lit(".csv"),
                )
            ),
        )
        .groupBy("file_name")
        .count()
        .withColumnRenamed("count", "raw_rows")
    )

    # Count rows in Bronze by original source filename.
    bronze_counts = (
        spark.table(BRONZE_TABLE)
        .withColumn(
            "file_name",
            file_name_from("_source_file"),
        )
        .groupBy("file_name")
        .count()
        .withColumnRenamed("count", "bronze_rows")
    )

    # Only compare batches that have already reached Bronze.
    comparison = (
        bronze_counts
        .join(raw_counts, on="file_name", how="left")
        .fillna(0, subset=["raw_rows"])
        .select("file_name", "raw_rows", "bronze_rows")
    )

    mismatches = comparison.filter(
        F.col("raw_rows") != F.col("bronze_rows")
    )

    passed = mismatches.limit(1).count() == 0

    print(f"Bronze batches checked: {comparison.count()}")
    print(
        f"Bronze rows checked: "
        f"{comparison.agg(F.sum('bronze_rows')).first()[0] or 0:,}"
    )
    print(f"Reconciliation passed: {passed}")

    if not passed:
        mismatches.show(truncate=False)

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
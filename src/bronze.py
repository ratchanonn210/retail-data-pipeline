
"""Incrementally ingest raw CSV files into a Bronze Delta table."""

from pyspark.sql import SparkSession, functions as F

from config import BRONZE_TABLE, LANDING

import re

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


def clean_column_names(df):
    """Replace invalid Delta characters in column names with underscores."""

    new_names = []
    for name in df.columns:
        cleaned = name.replace("%", "pct")
        cleaned = re.sub(r"[\s,;{}()=]+", "_", cleaned).strip("_")
        new_names.append(cleaned)

    return df.toDF(*new_names)


def create_bronze_table(spark: SparkSession) -> None:
    """Create an empty Delta table using the source CSV schema."""

    if spark.catalog.tableExists(BRONZE_TABLE):
        return

    raw_df = spark.read.options(**CSV_OPTIONS).csv(LANDING)
    raw_df = clean_column_names(raw_df)

    conflicts = METADATA_COLUMNS.intersection(raw_df.columns)
    if conflicts:
        raise ValueError(
            f"Source contains reserved metadata columns: {sorted(conflicts)}"
        )

    empty_df = (
        raw_df.limit(0)
        .withColumn(
            "_ingest_timestamp",
            F.lit(None).cast("timestamp"),
        )
        .withColumn(
            "_source_file",
            F.lit(None).cast("string"),
        )
        .withColumn(
            "_file_modified_at",
            F.lit(None).cast("timestamp"),
        )
    )
    
    temp_view = "bronze_empty_schema"
    empty_df.createOrReplaceTempView(temp_view)

    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {BRONZE_TABLE}
            USING DELTA
            AS SELECT * FROM {temp_view}
            WHERE 1 = 0
        """)
    finally:
        spark.catalog.dropTempView(temp_view)

    print(f"Bronze table created: {BRONZE_TABLE}") 


def load_bronze(spark: SparkSession) -> None:
    """Load new CSV files from Landing using COPY INTO."""

    format_options = ", ".join(
        f"'{key}' = '{value}'"
        for key, value in CSV_OPTIONS.items()
    )

    raw_columns = spark.read.options(**CSV_OPTIONS).csv(LANDING).columns
    clean_columns = clean_column_names(
        spark.createDataFrame([], schema=", ".join(f"`{c}` string" for c in raw_columns))
    ).columns

    select_columns = ", ".join(
        f"`{raw}` AS `{clean}`"
        for raw, clean in zip(raw_columns, clean_columns)
    )

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

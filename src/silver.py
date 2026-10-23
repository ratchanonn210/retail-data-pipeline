"""Clean, type, and deduplicate Bronze data into the Silver Delta table."""

from pyspark.sql import DataFrame, SparkSession, functions as F

from bronze import METADATA_COLUMNS
from config import BRONZE_TABLE, SILVER_TABLE, SPARK_DATE_FORMAT


def add_row_hash(df: DataFrame) -> DataFrame:
    """Add a hash of all business columns, used as a row identity."""
    business_columns = [c for c in df.columns if c not in METADATA_COLUMNS]
    parts = [F.coalesce(F.col(c), F.lit("")) for c in business_columns]
    return df.withColumn("_row_hash", F.sha2(F.concat_ws("||", *parts), 256))


def transform(bronze: DataFrame) -> DataFrame:
    """Cast types safely, remove duplicates, and flag invalid rows."""
    typed = add_row_hash(bronze).select(
        F.trim("CustomerID").alias("customer_id"),
        F.trim("ProductID").alias("product_id"),
        F.expr("try_cast(trim(Quantity) AS INT)").alias("quantity"),
        F.expr("try_cast(trim(Price) AS DECIMAL(10,2))").alias("price"),
        F.expr(
            f"try_to_timestamp(trim(TransactionDate), '{SPARK_DATE_FORMAT}')"
        ).alias("transaction_ts"),
        F.trim("PaymentMethod").alias("payment_method"),
        F.trim("StoreLocation").alias("store_location"),
        F.trim("ProductCategory").alias("product_category"),
        F.expr("try_cast(trim(DiscountApplied_pct) AS DOUBLE)").alias("discount_pct"),
        F.expr("try_cast(trim(TotalAmount) AS DECIMAL(12,2))").alias("total_amount"),
        "_row_hash",
        "_source_file",
        "_ingest_timestamp",
    )

    is_valid = (
        F.col("customer_id").isNotNull()
        & F.col("transaction_ts").isNotNull()
        & (F.col("quantity") > 0)
        & (F.col("price") >= 0)
        & F.col("total_amount").isNotNull()
    )

    return typed.dropDuplicates(["_row_hash"]).withColumn("is_valid", is_valid)


def build_silver(spark: SparkSession) -> int:
    """Append Bronze rows from files not yet in Silver. Return rows written."""
    bronze = spark.table(BRONZE_TABLE)

    if spark.catalog.tableExists(SILVER_TABLE):
        loaded_files = spark.table(SILVER_TABLE).select("_source_file").distinct()
        bronze = bronze.join(loaded_files, "_source_file", "left_anti")

    silver = transform(bronze)
    row_count = silver.count()

    if row_count > 0:
        silver.write.format("delta").mode("append").saveAsTable(SILVER_TABLE)

    return row_count


def check_silver(spark: SparkSession) -> bool:
    """Print basic quality metrics and fail on duplicate rows."""
    silver = spark.table(SILVER_TABLE)

    total = silver.count()
    invalid = silver.filter("NOT is_valid").count()
    duplicates = total - silver.select("_row_hash").distinct().count()

    print(f"Total rows : {total}")
    print(f"Invalid    : {invalid} ({invalid / total:.2%})")
    print(f"Duplicates : {duplicates}")

    return duplicates == 0
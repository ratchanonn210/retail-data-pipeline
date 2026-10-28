"""Clean, validate, and store retail transactions in Silver tables."""

from pyspark.sql import SparkSession, DataFrame, functions as F

from config import BRONZE_TABLE, CATALOG, SCHEMA


SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_transactions"
REJECTED_TABLE = f"{CATALOG}.{SCHEMA}.silver_rejected"
REQUIRED_COLUMNS = {
    "CustomerID",
    "ProductID",
    "Quantity",
    "Price",
    "TransactionDate",
    "PaymentMethod",
    "StoreLocation",
    "ProductCategory",
    "DiscountApplied_pct",
    "TotalAmount"
}


def validate_schema(df: DataFrame) -> None:
    """Check that Bronze contains all required columns."""
    missing_columns = REQUIRED_COLUMNS - set(df.columns)

    if missing_columns:
        raise ValueError(f"Missing required columns: {sorted(missing_columns)}")


def read_bronze(spark: SparkSession) -> DataFrame:
    return spark.table(BRONZE_TABLE)


def clean_data(df: DataFrame) -> DataFrame:
    string_columns = [
        "CustomerID",
        "ProductID",
        "Quantity",
        "Price",
        "TransactionDate",
        "PaymentMethod",
        "StoreLocation",
        "ProductCategory",
        "DiscountApplied_pct",
        "TotalAmount"
    ]

    for column in string_columns:
        df = df.withColumn(
            column,
            F.when(
                F.trim(F.col(column)) == "",
                F.lit(None),
            ).otherwise(F.trim(F.col(column)))
        )

    return (
        df
        .withColumn("Quantity", F.expr("try_cast(Quantity AS INT)"))
        .withColumn("Price", F.expr("try_cast(Price AS DOUBLE)"))
        .withColumn(
            "TransactionDate",
            F.coalesce(
                F.try_to_timestamp("TransactionDate", F.lit("M/d/yyyy H:mm")),
                F.try_to_timestamp("TransactionDate", F.lit("M/d/yyyy HH:mm")),
                F.try_to_timestamp("TransactionDate", F.lit("MM/dd/yyyy HH:mm"))
            )
        )
        .withColumn(
            "DiscountApplied_pct",
            F.expr("try_cast(regexp_replace(DiscountApplied_pct, '%', '') AS DOUBLE)")
        )
        .withColumn(
            "TotalAmount",
            F.expr("try_cast(TotalAmount AS DOUBLE)")
        )
    )


def validate_data(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split transactions into valid and rejected records."""
    invalid_conditions = [
        (F.col("CustomerID").isNull(), "missing_customer_id"),
        (F.col("ProductID").isNull(), "missing_product_id"),
        (F.col("Quantity").isNull(), "invalid_quantity"),
        (F.col("Quantity") <= 0, "quantity_not_positive"),
        (F.col("Price").isNull(), "invalid_price"),
        (F.col("Price") < 0, "negative_price"),
        (F.col("TransactionDate").isNull(), "invalid_transaction_date"),
        (F.col("DiscountApplied_pct").isNull(), "invalid_discount"),
        (F.col("DiscountApplied_pct") < 0, "discount_below_zero"),
        (F.col("DiscountApplied_pct") > 100, "discount_above_100"),
        (F.col("TotalAmount").isNull(), "invalid_total_amount"),
        (F.col("TotalAmount") < 0, "negative_total_amount")
    ]

    invalid_condition = F.lit(False)

    for condition, _ in invalid_conditions:
        invalid_condition = invalid_condition | condition

    rejection_reason = F.concat_ws(
        ", ",
        *[F.when(condition, F.lit(reason)) for condition, reason in invalid_conditions]
    )

    valid_df = df.filter(~invalid_condition)

    rejected_df = (
        df.filter(invalid_condition)
        .withColumn("rejection_reason", rejection_reason)
    )

    return valid_df, rejected_df


def save_silver(df: DataFrame) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(SILVER_TABLE)
    )


def save_rejected(df: DataFrame) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(REJECTED_TABLE)
    )


def reconcile(spark: SparkSession) -> bool:
    bronze_count = spark.table(BRONZE_TABLE).count()
    silver_count = spark.table(SILVER_TABLE).count()
    rejected_count = spark.table(REJECTED_TABLE).count()

    passed = bronze_count == silver_count + rejected_count

    print(f"Bronze rows: {bronze_count}")
    print(f"Silver rows: {silver_count}")
    print(f"Rejected rows: {rejected_count}")
    print(f"Reconciliation passed: {passed}")

    return passed


def get_table_snapshot(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    silver_df = spark.table(SILVER_TABLE)
    rejected_df = spark.table(REJECTED_TABLE)

    return silver_df, rejected_df


def check_idempotency(spark: SparkSession) -> bool:
    """Check whether rerunning Silver produces the same records."""
    bronze_df = read_bronze(spark)
    validate_schema(bronze_df)

    cleaned_df = clean_data(bronze_df)
    expected_silver, expected_rejected = validate_data(cleaned_df)

    expected_silver = expected_silver.cache()
    expected_rejected = expected_rejected.cache()

    try:
        expected_silver.count()
        expected_rejected.count()

        run_silver(spark)

        actual_silver, actual_rejected = get_table_snapshot(spark)

        silver_unchanged = (
            expected_silver.exceptAll(actual_silver).limit(1).count() == 0
            and actual_silver.exceptAll(expected_silver).limit(1).count() == 0
        )

        rejected_unchanged = (
            expected_rejected.exceptAll(actual_rejected).limit(1).count() == 0
            and actual_rejected.exceptAll(expected_rejected).limit(1).count() == 0
        )

        passed = silver_unchanged and rejected_unchanged

        print(f"Silver output unchanged: {silver_unchanged}")
        print(f"Rejected output unchanged: {rejected_unchanged}")
        print(f"Idempotency passed: {passed}")

        return passed

    finally:
        expected_silver.unpersist()
        expected_rejected.unpersist()


def run_silver(spark: SparkSession) -> None:
    bronze_df = read_bronze(spark)

    validate_schema(bronze_df)

    cleaned_df = clean_data(bronze_df)
    valid_df, rejected_df = validate_data(cleaned_df)

    save_silver(valid_df)
    save_rejected(rejected_df)

    print(f"Silver rows: {spark.table(SILVER_TABLE).count()}")
    print(f"Rejected rows: {spark.table(REJECTED_TABLE).count()}")

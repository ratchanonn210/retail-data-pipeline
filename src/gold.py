"""Build business-ready Gold aggregate tables from Silver."""

from pyspark.sql import SparkSession, DataFrame, functions as F

from config import CATALOG, SCHEMA
from silver import SILVER_TABLE


GOLD_DAILY_TABLE = f"{CATALOG}.{SCHEMA}.gold_daily_sales"
GOLD_PRODUCT_TABLE = f"{CATALOG}.{SCHEMA}.gold_product_performance"
GOLD_STORE_TABLE = f"{CATALOG}.{SCHEMA}.gold_store_performance"
GOLD_PAYMENT_TABLE = f"{CATALOG}.{SCHEMA}.gold_payment_performance"


def read_silver(spark: SparkSession) -> DataFrame:
    """Read validated transactions from Silver."""
    if not spark.catalog.tableExists(SILVER_TABLE):
        raise ValueError(f"Silver table does not exist: {SILVER_TABLE}")

    return spark.table(SILVER_TABLE)


def build_gold_tables(silver_df: DataFrame) -> dict[str, DataFrame]:
    """Build Gold aggregates from validated Silver records."""

    daily_sales = (
        silver_df
        .withColumn(
            "transaction_date",
            F.to_date("TransactionDate")
        )
        .groupBy("transaction_date")
        .agg(
            F.round(F.sum("TotalAmount"), 2).alias("sales_amount"),
            F.sum("Quantity").alias("units_sold"),
            F.count("*").alias("transaction_records")
        )
    )

    product_performance = (
        silver_df
        .groupBy("ProductID", "ProductCategory")
        .agg(
            F.round(F.sum("TotalAmount"), 2).alias("sales_amount"),
            F.sum("Quantity").alias("units_sold"),
            F.round(F.avg("Price"), 2).alias("avg_unit_price"),
            F.count("*").alias("transaction_records")
        )
    )

    store_performance = (
        silver_df
        .groupBy("StoreLocation")
        .agg(
            F.sum("TotalAmount").alias("sales_amount"),
            F.sum("Quantity").alias("units_sold"),
            F.count("*").alias("transaction_records")
        )
    )

    payment_performance = (
        silver_df
        .groupBy("PaymentMethod")
        .agg(
            F.sum("TotalAmount").alias("sales_amount"),
            F.sum("Quantity").alias("units_sold"),
            F.count("*").alias("transaction_records")
        )
    )

    return {
        GOLD_DAILY_TABLE: daily_sales,
        GOLD_PRODUCT_TABLE: product_performance,
        GOLD_STORE_TABLE: store_performance,
        GOLD_PAYMENT_TABLE: payment_performance
    }


def save_gold_tables(
    gold_tables: dict[str, DataFrame],
) -> None:
    """Replace Gold aggregate tables with the latest results."""
    for table_name, df in gold_tables.items():
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )


def run_gold(spark: SparkSession) -> None:
    """Build and save all Gold tables."""
    silver_df = read_silver(spark)
    gold_tables = build_gold_tables(silver_df)

    save_gold_tables(gold_tables)

    for table_name in gold_tables:
        row_count = spark.table(table_name).count()
        print(f"{table_name}: {row_count:,} rows")


def reconcile(spark: SparkSession) -> bool:
    """Check that Gold aggregates account for every Silver record."""
    silver_count = spark.table(SILVER_TABLE).count()

    tables = [
        GOLD_DAILY_TABLE,
        GOLD_PRODUCT_TABLE,
        GOLD_STORE_TABLE,
        GOLD_PAYMENT_TABLE
    ]

    results = {}

    for table_name in tables:
        if not spark.catalog.tableExists(table_name):
            raise ValueError(f"Gold table does not exist: {table_name}")

        gold_df = spark.table(table_name)

        record_count = gold_df.agg(
            F.sum("transaction_records")
        ).first()[0] or 0

        results[table_name] = record_count

    passed = all(
        record_count == silver_count
        for record_count in results.values()
    )

    print(f"Silver records: {silver_count:,}")

    for table_name, record_count in results.items():
        print(f"{table_name} records represented: {record_count:,}")

    print(f"Gold reconciliation passed: {passed}")

    return passed


def check_idempotency(spark: SparkSession) -> bool:
    """Check that rerunning Gold produces identical aggregate records."""
    silver_df = read_silver(spark)
    expected_tables = build_gold_tables(silver_df)

    # Cache expected outputs so both comparisons use the same results.
    for df in expected_tables.values():
        df.cache()
        df.count()

    try:
        run_gold(spark)

        results = {}

        for table_name, expected_df in expected_tables.items():
            actual_df = spark.table(table_name)

            expected_columns = sorted(expected_df.columns)
            actual_columns = sorted(actual_df.columns)

            if expected_columns != actual_columns:
                results[table_name] = False
                continue

            expected = expected_df.select(*expected_columns)
            actual = actual_df.select(*expected_columns)

            results[table_name] = (
                expected.exceptAll(actual).limit(1).count() == 0
                and actual.exceptAll(expected).limit(1).count() == 0
            )

        for table_name, passed in results.items():
            print(f"{table_name} unchanged: {passed}")

        passed = all(results.values())
        print(f"Gold idempotency passed: {passed}")

        return passed

    finally:
        for df in expected_tables.values():
            df.unpersist()
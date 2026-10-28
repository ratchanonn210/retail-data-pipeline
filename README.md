# Retail Data Engineering Pipeline

An end-to-end retail data engineering portfolio project built with Databricks, PySpark, and Delta Lake. This project demonstrates how raw retail transaction data can be ingested, cleaned, validated, and transformed into analytics-ready datasets using the Medallion Architecture.

##Project Overview

The pipeline follows a structured data processing workflow:

1. **Extract:** Download the source dataset from Kaggle.
2. **Seed Landing:** Split the source data into monthly CSV batches to simulate incoming files.
3. **Bronze:** Incrementally ingest raw files into Delta Lake using `COPY INTO`.
4. **Silver:** Clean data, convert data types, validate records, and isolate invalid transactions.
5. **Gold:** Aggregate validated transactions into business-oriented analytical tables.
6. **Validate:** Run reconciliation and idempotency checks to verify data consistency and repeatability.

## Tech Stack

- Python
- PySpark
- Databricks
- Delta Lake
- Git & GitHub

## Architecture

```text
             Kaggle Dataset
                    |
                    v
             Extract & Seed
                    |
                    v
                 Landing
                    |
                    v
             Bronze Layer
          Raw Delta Transactions
                    |
                    v
             Silver Layer
          Clean & Validate Data
                    |
             +------+------+
             |             |
             v             v
        Valid Records   Rejected Records
             |
             v
              Gold Layer
             Aggregations
                    |
       +------------+------------+
       |            |            |
       v            v            v
   Daily Sales   Product      Store &
                 Performance  Payment Analysis
```

**Data Source:** [Kaggle – Retail Transaction Dataset](https://www.kaggle.com/datasets/fahadrehman07/retail-transaction-dataset)

## Key Features

- **Medallion Architecture:** Separates raw ingestion, data cleaning, and business-level transformations into Bronze, Silver, and Gold layers.
- **Incremental File Ingestion:** Uses Delta Lake `COPY INTO` to ingest new files while avoiding reloading previously processed files.
- **Batch Ingestion Simulation:** Splits the source dataset into monthly CSV files and releases batches to simulate incoming data.
- **Data Quality Validation:** Checks required fields, data types, valid ranges, and other business rules.
- **Rejected Record Handling:** Stores invalid records separately with rejection reasons for investigation.
- **Data Reconciliation:** Compares record counts across pipeline stages to verify data completeness.
- **Idempotency Checks:** Verifies that repeated pipeline runs produce consistent results without introducing unintended duplicates.
- **Reusable Python Modules:** Separates pipeline logic from Databricks notebooks to improve code organization and maintainability.

## Project Structure

```text
retail-data-engineering-pipeline/
├── notebooks/
│   ├── 00_setup
│   ├── 01_extract
│   ├── 02_seed_landing
│   ├── 03_bronze
│   ├── 04_silver
│   └── 05_gold
├── src/
│   ├── config.py
│   ├── extract.py
│   ├── seed.py
│   ├── bronze.py
│   ├── silver.py
│   └── gold.py
├── .gitignore
└── README.md
```

## How to Run

Run the Databricks notebooks in the following order:

1. **`00_setup`** — Initialize the Unity Catalog schema and volume.
2. **`01_extract`** — Download the source dataset from Kaggle.
3. **`02_seed_landing`** — Split the dataset into monthly batches and release files to Landing.
4. **`03_bronze`** — Create and load the Bronze Delta table, then run reconciliation and idempotency checks.
5. **`04_silver`** — Clean and validate transactions, save valid and rejected records, and verify results.
6. **`05_gold`** — Generate aggregate tables and run reconciliation and idempotency checks.

## What I Learned

Through this project, I gained hands-on experience in:

- Designing an end-to-end data pipeline using the Medallion Architecture.
- Implementing incremental file ingestion with `COPY INTO`.
- Cleaning and validating data with PySpark.
- Handling invalid records and maintaining data quality.
- Implementing reconciliation and idempotency checks.
- Organizing reusable Python modules and Databricks notebooks.
- Using Git and GitHub to manage source code and track development changes.

This project strengthened my understanding of how data engineering components work together to transform raw data into reliable, analytics-ready datasets.

## Project Status

**Completed for the current project scope.**

This personal portfolio project focuses on batch data ingestion, data transformation, data quality, reconciliation, and idempotency. It is intended to demonstrate practical data engineering skills and provide a foundation for further improvements.
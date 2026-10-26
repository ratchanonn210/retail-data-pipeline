"""Shared configuration for the retail data pipeline."""

CATALOG = "workspace"
SCHEMA = "retail"
VOLUME = "landing"

BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

RAW_DIR = f"{BASE_PATH}/raw"
BATCHES_DIR = f"{BASE_PATH}/batches"
LANDING_DIR = f"{BASE_PATH}/incoming"

KAGGLE_HANDLE = "fahadrehman07/retail-transaction-dataset"
RAW_FILE = "Retail_Transaction_Dataset.csv"

DATE_COLUMN = "TransactionDate"
DATE_FORMAT = "%m/%d/%Y %H:%M"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_transactions"

CSV_OPTIONS = {
    "header": "true",
    "inferSchema": "false",
    "multiLine": "true",
    "quote": '"',
    "escape": '"',
}

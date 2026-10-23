"""Central configuration for the retail lakehouse pipeline."""

# Unity Catalog
CATALOG = "workspace"
SCHEMA = "retail"
VOLUME = "landing"

# Volume paths
BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

RAW_DIR = f"{BASE}/raw"
BATCHES = f"{BASE}/batches"
LANDING = f"{BASE}/incoming"

# Kaggle dataset
KAGGLE_HANDLE = "fahadrehman07/retail-transaction-dataset"
RAW_FILE = "Retail_Transaction_Dataset.csv"

# Batch simulation
DATE_COLUMN = "TransactionDate"
DATE_FORMAT = "%m/%d/%Y %H:%M"

# Delta tables
def table(name: str) -> str:
    """Return a fully qualified Unity Catalog table name."""
    return f"{CATALOG}.{SCHEMA}.{name}"


BRONZE_TABLE = table("bronze_transactions")

SILVER_TABLE = table("silver_transactions")

# Spark datetime pattern. Must describe the same format as DATE_FORMAT above.
SPARK_DATE_FORMAT = "M/d/yyyy H:mm"
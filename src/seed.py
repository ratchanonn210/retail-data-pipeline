"""Split the raw dataset into batches and release them to Landing."""

import os
import shutil

import pandas as pd

from config import (
    BATCHES_DIR,
    DATE_COLUMN,
    DATE_FORMAT,
    LANDING_DIR,
    RAW_DIR,
    RAW_FILE,
)


def split_into_batches() -> int:
    """Split the raw CSV into monthly CSV files."""
    source_path = os.path.join(RAW_DIR, RAW_FILE)

    if not os.path.isfile(source_path):
        raise FileNotFoundError(f"Raw file not found: {source_path}")

    df = pd.read_csv(source_path, dtype=str)

    if DATE_COLUMN not in df.columns:
        raise ValueError(f"Missing required column: {DATE_COLUMN}")

    dates = pd.to_datetime(
        df[DATE_COLUMN],
        format=DATE_FORMAT,
        errors="coerce",
    )

    df["_batch_month"] = dates.dt.strftime("%Y-%m")
    df["_batch_month"] = df["_batch_month"].fillna("unknown")

    if os.path.exists(BATCHES_DIR):
        shutil.rmtree(BATCHES_DIR)

    os.makedirs(BATCHES_DIR, exist_ok=True)

    for month, batch_df in df.groupby("_batch_month", sort=True):
        output_path = os.path.join(
            BATCHES_DIR,
            f"transactions_{month}.csv",
        )

        batch_df.drop(columns="_batch_month").to_csv(
            output_path,
            index=False,
        )

    return len(df)


def release_batches(n: int) -> list[str]:
    """Copy up to n unreleased batch files into Landing."""
    if n < 0:
        raise ValueError("n must be zero or greater")

    if not os.path.isdir(BATCHES_DIR):
        raise FileNotFoundError("Batch directory not found. Run split_into_batches() first.")

    os.makedirs(LANDING_DIR, exist_ok=True)

    released_files = set(os.listdir(LANDING_DIR))

    available_batches = sorted(
        filename
        for filename in os.listdir(BATCHES_DIR)
        if filename.endswith(".csv")
        and filename not in released_files
    )

    selected_batches = available_batches[:n]

    for filename in selected_batches:
        shutil.copy2(
            os.path.join(BATCHES_DIR, filename),
            os.path.join(LANDING_DIR, filename),
        )

    return selected_batches
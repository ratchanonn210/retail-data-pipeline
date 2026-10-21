
"""Simulate incremental file arrivals in the landing zone."""

import os
import shutil

import pandas as pd

from config import (
    BATCHES,
    DATE_COLUMN,
    DATE_FORMAT,
    LANDING,
    RAW_DIR,
    RAW_FILE,
)

UNPARSEABLE_BATCH = "unknown"


def split_into_batches() -> int:
    """Split raw CSV into monthly batches and return row count."""

    raw_path = os.path.join(RAW_DIR, RAW_FILE)

    if not os.path.isfile(raw_path):
        raise FileNotFoundError(f"Source CSV not found: {raw_path}")

    raw = pd.read_csv(raw_path, dtype=str)

    if DATE_COLUMN not in raw.columns:
        raise ValueError(
            f"Missing required date column: {DATE_COLUMN}"
        )

    parsed_dates = pd.to_datetime(
        raw[DATE_COLUMN],
        format=DATE_FORMAT,
        errors="coerce",
    )

    batch_keys = (
        parsed_dates.dt.strftime("%Y-%m")
        .fillna(UNPARSEABLE_BATCH)
    )

    # Rebuild batch files for a fresh simulation.
    shutil.rmtree(BATCHES, ignore_errors=True)
    os.makedirs(BATCHES, exist_ok=True)

    for key, batch in raw.groupby(batch_keys, sort=True):
        output_path = os.path.join(
            BATCHES,
            f"transactions_{key}.csv",
        )
        batch.to_csv(output_path, index=False)

    return len(raw)


def release_batches(n: int) -> list[str]:
    """Copy the next n unreleased CSV batches into LANDING."""

    if n < 0:
        raise ValueError("n must be non-negative")

    if not os.path.isdir(BATCHES):
        raise FileNotFoundError(
            f"Batch directory not found: {BATCHES}. "
            "Run split_into_batches() first."
        )

    os.makedirs(LANDING, exist_ok=True)

    already_released = set(os.listdir(LANDING))

    pending = [
        filename
        for filename in sorted(os.listdir(BATCHES))
        if filename.endswith(".csv")
        and filename not in already_released
    ]

    released = pending[:n]

    for filename in released:
        shutil.copy2(
            os.path.join(BATCHES, filename),
            os.path.join(LANDING, filename),
        )

    return released

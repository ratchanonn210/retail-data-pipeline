"""Download the source dataset from Kaggle."""

import os
import shutil

import kagglehub

from config import KAGGLE_HANDLE, RAW_DIR, RAW_FILE


def download_dataset() -> str:
    """Download the Kaggle dataset and return its local directory."""
    return kagglehub.dataset_download(KAGGLE_HANDLE)


def copy_to_raw_dir(source_dir: str) -> str:
    """Copy the source CSV into the raw directory."""
    source_path = os.path.join(source_dir, RAW_FILE)

    if not os.path.isfile(source_path):
        raise FileNotFoundError(
            f"Source file not found: {source_path}"
        )

    if os.path.getsize(source_path) == 0:
        raise ValueError(f"Source file is empty: {source_path}")

    os.makedirs(RAW_DIR, exist_ok=True)
    destination_path = os.path.join(RAW_DIR, RAW_FILE)

    if os.path.abspath(source_path) == os.path.abspath(destination_path):
        return destination_path

    shutil.copy2(source_path, destination_path)
    return destination_path

"""Download the Kaggle dataset into the raw directory."""

import os
import shutil

import kagglehub

from config import KAGGLE_HANDLE, RAW_DIR, RAW_FILE


def download_dataset() -> str:
    """Download the dataset and return its local cache path."""
    return kagglehub.dataset_download(KAGGLE_HANDLE)


def copy_to_raw_dir(source_dir: str) -> str:
    """Copy the expected CSV file into RAW_DIR."""

    if not os.path.isdir(source_dir):
        raise FileNotFoundError(
            f"Dataset directory not found: {source_dir}"
        )

    source_path = os.path.join(source_dir, RAW_FILE)

    if not os.path.isfile(source_path):
        raise FileNotFoundError(
            f"Expected CSV file not found: {RAW_FILE}"
        )

    os.makedirs(RAW_DIR, exist_ok=True)

    destination_path = os.path.join(RAW_DIR, RAW_FILE)
    shutil.copy2(source_path, destination_path)

    return destination_path
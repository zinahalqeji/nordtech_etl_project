"""
extract.py

Handles all data extraction tasks such as loading CSV files.
"""

from pathlib import Path
import pandas as pd
from src.config import RAW_MAIN, RAW_VAL


# --------------------------------------------------
# Generic CSV loader
# --------------------------------------------------


def load_csv(file_path: str) -> pd.DataFrame:
    """
    Load a CSV file into a pandas DataFrame.

    Parameters
    ----------
    file_path : str
        Path to the CSV file.

    Returns
    -------
    pd.DataFrame
        Loaded dataset.
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"[EXTRACT] File not found: {file_path}")

    df = pd.read_csv(file_path)
    print(f"[EXTRACT] Loaded CSV: {file_path} (rows={len(df)}, cols={len(df.columns)})")
    return df


# --------------------------------------------------
# Main extraction functions for the ETL pipeline
# --------------------------------------------------


def load_main_data() -> pd.DataFrame:
    """
    Load the main raw dataset defined in config.py.
    """
    print(f"[EXTRACT] Loading main dataset from: {RAW_MAIN}")
    return load_csv(RAW_MAIN)


def load_validation_data() -> pd.DataFrame:
    """
    Load the validation dataset defined in config.py.
    """
    print(f"[EXTRACT] Loading validation dataset from: {RAW_VAL}")
    return load_csv(RAW_VAL)

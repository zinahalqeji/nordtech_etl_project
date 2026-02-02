"""
extract.py

Handles all data extraction tasks such as loading CSV files.
"""

from pathlib import Path
import pandas as pd


# --------------------------------------------------
# Generic CSV loader
# --------------------------------------------------


def load_csv(file_path: Path) -> pd.DataFrame:
    """
    Load a CSV file into a pandas DataFrame.
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_csv(file_path)
    print(f"[EXTRACT] Loaded CSV: {file_path} (rows={len(df)}, cols={len(df.columns)})")
    return df


# --------------------------------------------------
# Main extraction function for the ETL pipeline
# --------------------------------------------------


def load_raw_data(path: str) -> pd.DataFrame:
    """
    Load a raw dataset from a given file path.
    This allows the pipeline to run on both main and validation datasets.
    """
    file_path = Path(path)
    print(f"[EXTRACT] Loading raw dataset from: {file_path}")
    return load_csv(file_path)

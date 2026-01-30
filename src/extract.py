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


def load_raw_data() -> pd.DataFrame:
    """
    Load the raw Nordtech dataset from data/raw.
    """
    root = Path(__file__).resolve().parents[1]
    raw_path = root / "data" / "raw" / "nordtech_data.csv"

    print(f"[EXTRACT] Loading raw dataset from: {raw_path}")
    return load_csv(raw_path)

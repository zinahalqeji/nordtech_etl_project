"""
extract.py

Handles all data extraction tasks such as loading CSV files.
"""

from pathlib import Path
import pandas as pd


def load_csv(file_path: Path) -> pd.DataFrame:
    """
    Load a CSV file into a pandas DataFrame.

    Parameters
    ----------
    file_path : Path
        Path to the CSV file.

    Returns
    -------
    pd.DataFrame
        Loaded dataset.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_csv(file_path)
    print(f"Loaded CSV: {file_path} (rows={len(df)}, cols={len(df.columns)})")
    return df

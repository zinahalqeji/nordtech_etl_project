"""
load.py

Handles saving cleaned data to CSV and SQLite.
"""

import pandas as pd
import sqlite3
from pathlib import Path
from src.config import CLEANED, DB_PATH, TABLE_NAME


# --------------------------------------------------
# Save to CSV
# --------------------------------------------------


def save_cleaned_csv(df: pd.DataFrame, output_path: str = CLEANED) -> Path:
    """
    Save the cleaned dataset to a CSV file defined in config.py.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"[LOAD] CSV saved successfully → {output_path}")

    return output_path


# --------------------------------------------------
# Save to SQLite
# --------------------------------------------------


def load_to_sqlite(
    df: pd.DataFrame,
    db_path: str = DB_PATH,
    table_name: str = TABLE_NAME,
) -> None:
    """
    Save the cleaned dataset to a SQLite database.
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)

    print(f"[LOAD] SQLite table '{table_name}' updated successfully → {db_path}")


# --------------------------------------------------
# Main load function
# --------------------------------------------------


def load_clean_data(df: pd.DataFrame) -> None:
    """
    Save cleaned data to both CSV and SQLite.
    """
    print("[LOAD] Starting load process...")
    save_cleaned_csv(df)
    load_to_sqlite(df)
    print("[LOAD] Load process completed.")

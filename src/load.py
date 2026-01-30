from __future__ import annotations
import pandas as pd
import sqlite3
from pathlib import Path

# --------------------------------------------------
# Configuration
# --------------------------------------------------

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
PROCESSED_DIR = DATA_DIR / "processed"
DB_DIR = ROOT_DIR / "database"
DB_PATH = DB_DIR / "nordtech.db"
TABLE_NAME = "clean_orders"


# --------------------------------------------------
# Save to CSV
# --------------------------------------------------


def save_to_csv(df: pd.DataFrame, filename: str = "nordtech_cleaned.csv") -> Path:
    """
    Save the cleaned DataFrame to data/processed.
    """
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    output_path = PROCESSED_DIR / filename
    df.to_csv(output_path, index=False, encoding="utf-8")

    print(f"[LOAD] CSV saved successfully → {output_path}")
    return output_path


# --------------------------------------------------
# Save to SQLite
# --------------------------------------------------


def save_to_sqlite(
    df: pd.DataFrame, db_path: Path = DB_PATH, table: str = TABLE_NAME
) -> None:
    """
    Save the cleaned DataFrame into a SQLite database.
    """
    DB_DIR.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        df.to_sql(table, conn, if_exists="replace", index=False)

    print(f"[LOAD] SQLite table '{table}' updated successfully → {db_path}")


# --------------------------------------------------
# Main load function
# --------------------------------------------------


def load_clean_data(df: pd.DataFrame) -> None:
    """
    Save cleaned data to both CSV and SQLite.
    """
    print("[LOAD] Starting load process...")
    save_to_csv(df)
    save_to_sqlite(df)
    print("[LOAD] Load process completed.")

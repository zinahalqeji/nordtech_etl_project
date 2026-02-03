from __future__ import annotations
import pandas as pd

from src.extract import load_raw_data
from src.transform import transform_data
from src.load import load_clean_data
from src.sentiment import add_sentiment_column


def run_pipeline(path: str, table_name: str) -> None:
    print("\n==============================")
    print(f"🚀 Running ETL Pipeline for: {path}")
    print("==============================\n")

    # Extract
    print("[1/3] Extracting raw data...")
    df_raw = load_raw_data(path)
    print(f"[EXTRACT] Raw rows loaded: {len(df_raw)}")

    # Transform
    print("\n[2/3] Transforming data...")
    df_clean = transform_data(df_raw)
    print(f"[TRANSFORM] Cleaned rows: {len(df_clean)}")

    # Sentiment Analysis
    print("\n[2.5/3] Adding sentiment scores (BERT)...")
    df_clean = add_sentiment_column(df_clean, text_column="recension_text")
    print("[SENTIMENT] Sentiment scores added successfully")

    # Load
    print("\n[3/3] Loading cleaned data into SQLite...")
    load_clean_data(df_clean, table_name=table_name)

    print("\n==============================")
    print("✅ ETL Pipeline Completed Successfully 👌😊")
    print("==============================\n")


if __name__ == "__main__":
    # Main dataset
    run_pipeline(path="data/raw/nordtech_data.csv", table_name="orders_clean")

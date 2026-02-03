"""
run_pipeline.py

Main entry point for the Nordtech ETL pipeline.
"""

import pandas as pd

from src.extract import load_main_data
from src.transform import transform_data
from src.load import load_clean_data
from src.sentiment import add_sentiment_column
from src.config import RAW_MAIN


def run_pipeline() -> None:
    print("\n==============================")
    print(f"🚀 Running ETL Pipeline for: {RAW_MAIN}")
    print("==============================\n")

    # Extract
    print("[1/3] Extracting raw data...")
    df_raw = load_main_data()
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
    print("\n[3/3] Loading cleaned data into CSV + SQLite...")
    load_clean_data(df_clean)

    print("\n==============================")
    print("✅ ETL Pipeline Completed Successfully 👌😊")
    print("==============================\n")


if __name__ == "__main__":
    run_pipeline()

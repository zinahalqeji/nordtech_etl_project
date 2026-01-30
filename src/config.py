"""
config.py

Central configuration for file paths and database settings.
All paths are defined relative to the project root.
"""

# Raw data
RAW_MAIN = "data/raw/nordtech_data.csv"
RAW_VAL = "data/raw/nordtech_validation.csv"

# Processed data (optional CSV export)
CLEANED = "data/processed/nordtech_cleaned.csv"

# Database
DB_PATH = "database/nordtech.db"
TABLE_NAME = "clean_orders"

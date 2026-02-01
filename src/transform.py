"""
transform.py

This module contains all data cleaning and transformation logic for the
Nordtech ETL pipeline. Each function handles a specific cleaning task,
and the `clean_all()` function orchestrates the full transformation
pipeline in a clear, maintainable sequence.
"""

from __future__ import annotations
import pandas as pd
import numpy as np


# ---------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------


def _safe_str(col: pd.Series) -> pd.Series:
    """
    Convert a column to lowercase, stripped strings safely.
    """
    return col.astype(str).str.strip().str.lower()


# ---------------------------------------------------------
# Cleaning functions
# ---------------------------------------------------------


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names: lowercase, underscores, stripped.
    """
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df


def clean_id_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean ID-like columns by converting to string and stripping whitespace.
    """
    id_cols = ["order_id", "orderrad_id", "kund_id", "produkt_sku"]

    for col in id_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    return df


def clean_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert date columns to datetime, including Swedish month names.
    Uses _safe_str() to normalize text before parsing.
    """

    swedish_months = {
        "januari": "01",
        "februari": "02",
        "mars": "03",
        "april": "04",
        "maj": "05",
        "juni": "06",
        "juli": "07",
        "augusti": "08",
        "september": "09",
        "oktober": "10",
        "november": "11",
        "december": "12",
    }

    date_cols = ["orderdatum", "leveransdatum", "recensionsdatum"]

    for col in date_cols:
        if col not in df.columns:
            continue

        # Normalize text using your helper
        col_series = _safe_str(df[col])

        # Replace Swedish month names with month numbers
        for swe, num in swedish_months.items():
            col_series = col_series.str.replace(swe, num, regex=False)

        # Convert "5 12 2024" → "5-12-2024"
        col_series = col_series.str.replace(" ", "-", regex=False)

        # Parse as datetime (day-first format)
        df[col] = pd.to_datetime(col_series, errors="coerce", dayfirst=True)

    return df


def fix_reversed_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix rows where delivery date is earlier than order date.
    """
    if "orderdatum" not in df.columns or "leveransdatum" not in df.columns:
        return df

    mask = df["leveransdatum"] < df["orderdatum"]
    df.loc[mask, ["orderdatum", "leveransdatum"]] = df.loc[
        mask, ["leveransdatum", "orderdatum"]
    ].values

    return df


def clean_prices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean price column by removing currency symbols and converting to float.
    """
    if "pris_per_enhet" not in df.columns:
        return df

    col = (
        df["pris_per_enhet"]
        .astype(str)
        .str.replace(" ", "")
        .str.replace("SEK", "", case=False)
        .str.replace("kr", "", case=False)
        .str.replace(":-", "")
        .str.replace(",", ".")
    )

    df["pris_per_enhet"] = pd.to_numeric(col, errors="coerce")
    return df


def clean_region(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize region names and fix common misspellings.
    """
    if "region" not in df.columns:
        return df

    mapping = {
        "sthlm": "stockholm",
        "sthml": "stockholm",
        "gothenburg": "göteborg",
        "gbgb": "göteborg",
        "gbg": "göteborg",
        "linkoping": "linköping",
        "malmo": "malmö",
        "orebro": "örebro",
        "vasteras": "västerås",
        "norr": "norrland",
    }

    col = _safe_str(df["region"]).replace("nan", None).replace(mapping)
    df["region"] = col

    return df


def clean_payment(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize payment method names.
    """
    if "betalmetod" not in df.columns:
        return df

    mapping = {
        "kort": "card",
        "kreditkort": "card",
        "visa": "card",
        "mastercard": "card",
        "swish": "swish",
        "mobilbetalning": "swish",
        "faktura": "invoice",
    }

    col = _safe_str(df["betalmetod"]).replace("nan", None).replace(mapping)
    df["betalmetod"] = col.fillna("unknown")

    return df


def clean_antal(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean quantity column by converting Swedish words and removing noise.
    """
    if "antal" not in df.columns:
        return df

    word_map = {
        "en": 1,
        "ett": 1,
        "två": 2,
        "tva": 2,
        "tre": 3,
        "fyra": 4,
        "fem": 5,
        "sex": 6,
        "sju": 7,
        "åtta": 8,
        "atta": 8,
        "nio": 9,
        "tio": 10,
    }

    col = _safe_str(df["antal"])
    col = col.str.replace('"', "", regex=False)
    col = col.str.replace("st", "", regex=False).str.strip()
    col = col.replace(word_map)

    df["antal"] = pd.to_numeric(col, errors="coerce").fillna(1)

    return df


def clean_kundtyp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize customer type labels.
    """
    if "kundtyp" not in df.columns:
        return df

    mapping = {
        "privat": "private",
        "konsument": "private",
        "b2c": "private",
        "företag": "business",
        "firma": "business",
        "b2b": "business",
    }

    col = _safe_str(df["kundtyp"]).replace(mapping)
    df["kundtyp"] = col

    return df


def clean_leveransstatus(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize delivery status labels.
    """
    if "leveransstatus" not in df.columns:
        return df

    mapping = {
        "levererad": "delivered",
        "mottagen": "received",
        "skickad": "sent",
        "under transport": "in_transit",
        "på väg": "in_transit",
        "pa väg": "in_transit",
        "pa vag": "in_transit",
        "retur": "returned",
        "returnerad": "returned",
        "återsänd": "returned",
        "atersand": "returned",
    }

    col = _safe_str(df["leveransstatus"]).replace(mapping)
    col = col.replace(["nan", "none", ""], "unknown")

    df["leveransstatus"] = col
    return df


def clean_betyg(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean rating column: numeric, clipped, median-filled.
    """
    if "betyg" not in df.columns:
        return df

    col = pd.to_numeric(df["betyg"], errors="coerce")
    col = col.clip(1, 5)
    df["betyg"] = col.fillna(col.median())

    return df


def clean_recension_text(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean review text column by removing placeholder strings.
    """
    if "recension_text" not in df.columns:
        return df

    col = df["recension_text"].astype(str).str.strip()
    col = col.replace(["nan", "none", "null", "na", ""], np.nan)

    df["recension_text"] = col
    return df


def remove_duplicates(
    df: pd.DataFrame, unique_keys: list[str] | None = None
) -> pd.DataFrame:
    """
    Remove duplicate rows. If unique keys are provided, enforce uniqueness.
    """
    df = df.drop_duplicates()

    if unique_keys:
        df = df.drop_duplicates(subset=unique_keys, keep="first")

    return df


# ---------------------------------------------------------
# Full pipeline
# ---------------------------------------------------------


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run the full transformation pipeline on the raw dataset.
    Applies all cleaning functions in the correct order.
    """
    print("[TRANSFORM] Starting transformation pipeline...")

    df = clean_column_names(df)
    df = clean_id_columns(df)
    df = clean_date(df)
    df = fix_reversed_dates(df)
    df = clean_kundtyp(df)
    df = clean_antal(df)
    df = clean_prices(df)
    df = clean_payment(df)
    df = clean_leveransstatus(df)
    df = clean_region(df)
    df = clean_betyg(df)
    df = clean_recension_text(df)
    df = remove_duplicates(df)

    print("[TRANSFORM] Transformation pipeline completed.")
    return df

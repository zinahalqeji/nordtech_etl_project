import pandas as pd
import numpy as np


def clean_date(df: pd.DataFrame) -> pd.DataFrame:
    """Convert date columns to datetime."""
    date_cols = ["orderdatum", "leveransdatum", "recensionsdatum"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def clean_prices(df: pd.DataFrame) -> pd.DataFrame:
    """Clean price column by removing currency symbols and converting to float.""" 
    if "pris_per_enhet" not in df.columns:
        return df
    col = (
        df["pris_per_enhet"] .astype(str) .str.replace(" ", "") .str.replace("SEK", "", case=False) .str.replace("kr", "", case=False) .str.replace(":-", "") .str.replace(",", ".")
        )
    df["pris_per_enhet"] = pd.to_numeric(col, errors="coerce")
    return df
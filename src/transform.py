import pandas as pd
import numpy as np


def clean_date(df: pd.DataFrame) -> pd.DataFrame:
    """Convert date columns to datetime."""
    date_cols = ["orderdatum", "leveransdatum", "recensionsdatum"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df

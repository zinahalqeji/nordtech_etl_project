import pandas as pd



# ---------------------------------------------------------
#  Helper utilities
# ----------------------------------------------------------

def _safe_str(col: pd.Series) -> pd.Series:
    """Convert a column to lowercase, stripped strings safely."""
    return col.astype(str).str.strip().str.lower()


# ---------------------------------------------------------
#  Data cleaning functions
# ---------------------------------------------------------


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
    """Normalize region names and fix common misspellings."""
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
    """Normalize payment method names."""
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
    col = _safe_str(df["betalmetod"]).replace("nan", None).replace(mapping).replace(["nan", "none", ""], "unknown")
    df["betalmetod"] = col
    return df

def clean_leveransstatus(df: pd.DataFrame) -> pd.DataFrame:
    
    """Normalize delivery status labels."""
    
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
    col = _safe_str(df["leveransstatus"]).replace(mapping).replace(["nan", "none", ""], "unknown")
    df["leveransstatus"] = col
    return df


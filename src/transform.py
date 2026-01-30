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

def clean_antal(df: pd.DataFrame) -> pd.DataFrame:
    """Clean quantity column by converting Swedish words and removing noise.""" 
    if "antal" not in df.columns:
        return df
    word_map = {
        "en": 1, "ett": 1, "två": 2, "tva": 2, "tre": 3, "fyra": 4, "fem": 5, "sex": 6, "sju": 7, "åtta": 8,"atta": 8, "nio": 9, "tio": 10,
        }
    col = _safe_str(df["antal"])
    col = col.str.replace('"', "", regex=False)
    col = col.str.replace("st", "", regex=False).str.strip()
    col = col.replace(word_map)
    df["antal"] = pd.to_numeric(col, errors="coerce").fillna(1)
    return df

def clean_kundtyp(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize customer type labels."""
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

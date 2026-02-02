"""
sentiment.py

BERT-based sentiment analysis for the NordTech ETL project.
Classifies recensioner as positive / neutral / negative.

Uses a multilingual BERT model from Hugging Face.
"""

import pandas as pd
from typing import Optional
from transformers import pipeline

# Initialize model once (global) so it's reused
_sentiment_model = pipeline(
    "sentiment-analysis", model="nlptown/bert-base-multilingual-uncased-sentiment"
)


def compute_sentiment_label(text: Optional[str]) -> str:
    """
    Compute sentiment category (positive/neutral/negative) for a single text.

    Args:
        text (str or None): Review text.

    Returns:
        str: "positive", "neutral", or "negative".
    """
    if not isinstance(text, str) or text.strip() == "":
        return "neutral"

    # Truncate very long texts to avoid performance issues
    text = text[:512]

    result = _sentiment_model(text)[0]  # e.g. {'label': '4 stars', 'score': 0.98}
    label = result["label"]

    # Map star labels to sentiment categories
    if label in ["4 stars", "5 stars"]:
        return "positive"
    elif label in ["1 star", "2 stars"]:
        return "negative"
    else:
        return "neutral"


def add_sentiment_column(
    df: pd.DataFrame, text_column: str = "recension_text"
) -> pd.DataFrame:
    """
    Add a sentiment_category column to the DataFrame based on recension_text.

    Args:
        df (pd.DataFrame): Input DataFrame.
        text_column (str): Column containing review text.

    Returns:
        pd.DataFrame: DataFrame with new 'sentiment_category' column.
    """
    if text_column not in df.columns:
        raise ValueError(f"Column '{text_column}' not found in DataFrame.")

    df = df.copy()
    df["sentiment_category"] = df[text_column].apply(compute_sentiment_label)
    return df


if __name__ == "__main__":
    example = "Den här produkten är jättebra!"
    print("Example text:", example)
    print("Predicted sentiment:", compute_sentiment_label(example))

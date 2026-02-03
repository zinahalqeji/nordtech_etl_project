# 📘 DATA DICTIONARY — Nordtech Cleaned Dataset

This document describes all columns in the cleaned dataset (`clean_orders`) and the transformations applied to each during the ETL process.

---

## 🗂️ Table: `clean_orders`

| Column Name    | Type      | Description                           | Transformations Applied                  |
|----------------|-----------|---------------------------------------|------------------------------------------|
| order_id       | string    | Unique order identifier               | Converted to string, stripped whitespace |
| orderrad_id    | string    | Unique order‑line identifier          | Converted to string, stripped whitespace |
| kund_id        | string    | Unique customer identifier            | Converted to string, stripped whitespace |
| produkt_sku    | string    | Product SKU code                      | Converted to string, stripped whitespace |
| orderdatum     | datetime  | Date when the order was placed        | Parsed from mixed formats (ISO, slash,   |
                                                                     |  Swedish month names), normalized to datetime, reversed dates fixed          
| leveransdatum  | datetime  | Date when the order was delivered     | Same parsing as `orderdatum`, corrected  |
                                                                     | if earlier than order date               |

| leveransstatus | string    | Delivery status                       | Lowercased, stripped, mapped to 
                                                                     | standardized labels (e.g., “levererad” →  “delivered”), unknown values set to “unknown” |
| region         | string    | Customer region                       | Lowercased, stripped, corrected 
                                                                     | misspellings (e.g., “sthlm” → “stockholm”, “gbg” → “göteborg”)           |
| produktnamn    | string    | Product name                          | Lowercased, stripped                     |
| kategori       | string    | Product category                      | Lowercased, stripped                     |
| antal          | int       | Quantity purchased                    | Swedish words converted to numbers 
                                                                     |(“två”, “tre”), removed “st”, cleaned quotes, coerced to numeric, missing values set to 1                                   |
| pris_per_enhet | float     | Price per unit (SEK)                  | Removed “kr”, “SEK”, “:-”, removed 
                                                                       spaces, replaced comma with dot, converted to float                       |
| total_price    | float     | Total order value                     | Created column: `antal * pris_per_enhet` |
| kundtyp        | string    | Customer type                         | Normalized to “private” or “business”
                                                                     | (e.g., “privat”, “b2c”, “företag”)       |
| betalmetod     | string    | Payment method                        | Normalized to “card”, “swish”, or 
                                                                     | “invoice”; unknown values set to “unknown”                                  |
| betyg          | float     | Customer rating (1–5)                 | Converted to numeric, clipped to 1–5,
                                                                     | missing values filled with median        |
| recension_text | string    | Customer review text                  | Stripped whitespace, replaced 
                                                                     | placeholders (“nan”, “null”, “na”) with NaN                                        |
| recensionsdatum| datetime  | Review date                           | Parsed using same date logic as order 
                                                                     | dates                                    |
| sentiment_category | string | Sentiment label (positive / neutral / negative) | Generated using BERT model
                                                                     | (1–5 stars mapped to 3 categories)       |
| delivery_days  | int       | Delivery time in days                 | Created column: `leveransdatum` - 
                                                                     | `orderdatum`                             |
| month          | string    | Month of order                        | Extracted from `orderdatum` |
| weekday        | string    | Weekday of order                      | Extracted from `orderdatum` |
| has_review     | bool      | Whether a review exists               | Created column: `recension_text.notna()` |

---

## 📝 General Transformations Applied

- All column names standardized to lowercase with underscores  
- Duplicate rows removed  
- All string fields stripped and normalized  
- Swedish characters preserved and standardized  
- All date fields converted to proper datetime objects  
- All numeric fields coerced to numeric types  
- Sentiment labels generated using a multilingual BERT model  
- Additional KPI‑supporting columns created (`delivery_days`, `month`, `weekday`, `has_review`)  


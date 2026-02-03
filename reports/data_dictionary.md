# 📘 DATA DICTIONARY — Nordtech Cleaned Dataset

This table describes the cleaned dataset produced by the Nordtech ETL pipeline.

---

## 🗂️ Table: `clean_orders`

| Column Name        | Type      | Description                          |
|--------------------|-----------|--------------------------------------|
| order_id           | int       | Unique order identifier              |
| kund_id            | int       | Unique customer identifier           |
| orderdatum         | datetime  | Order date                           |
| leveransdatum      | datetime  | Delivery date                        |
| leveransstatus     | string    | Delivery status                      |
| region             | string    | Customer region                      |
| produktnamn        | string    | Product name                         |
| kategori           | string    | Product category                     |
| antal              | int       | Quantity purchased                   |
| pris_per_enhet     | float     | Price per unit (SEK)                 |
| total_price        | float     | Total order value                    |
| kundtyp            | string    | Customer type                        |
| betalmetod         | string    | Payment method                       |
| betyg              | float     | Customer rating (1–5)                |
| recension_text     | string    | Customer review text                 |
| recensionsdatum    | datetime  | Review date                          |
| sentiment_category | string    | Sentiment label (BERT)               |
| delivery_days      | int       | Delivery time in days                |
| month              | period    | Month extracted from orderdatum      |
| weekday            | string    | Weekday extracted from orderdatum    |
| has_review         | bool      | Whether a review exists              |

---

## Notes

- `total_price`, `delivery_days`, `month`, `weekday`, and `has_review` were created during transformation/KPI analysis.  
- All date fields were converted to datetime format.  
- Text fields were cleaned and standardized.  
- Sentiment labels were generated using a BERT model.  
- `betalmetod` was cleaned and standardized (e.g., “Kort”, “Swish”, “Faktura”).  

# 🧰 Nordtech ETL Pipeline — Developer Guide

A complete, modular ETL pipeline for Nordtech’s e‑commerce dataset.  
This project extracts, cleans, transforms, enriches, and loads data into a SQLite database, applies BERT sentiment analysis, and generates business‑ready KPIs and visualizations.

This README is written as a **developer‑style tutorial** so anyone can run and understand the pipeline.

---

## 📁 Project Structure

```
NORDTECH_ETL_PROJECT/
│
├── data/
│   ├── raw/
│   │   ├── nordtech_data.csv
│   │   └── nordtech_validation.csv
│   └── processed/
│       └── nordtech_cleaned.csv
│
├── database/
│   └── nordtech.db
│
├── notebooks/
│   ├── 01_eda.ipynb
│   ├── 02_pipeline_dev.ipynb
│   └── 03_kpi_analysis.ipynb
│
├── reports/
│   ├── data_dictionary.md
│   └── reflection.pdf
│
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── extract.py
│   ├── transform.py
│   ├── sentiment.py
│   ├── load.py
│   └── __pycache__/
│
├── run_pipeline.py
├── requirements.txt
└── README.md
```

---

## ⚙️ Installation

### 1. Create and activate a virtual environment

```bash
python -m venv venv
venv\Scripts\activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

---

## 🚀 Run the ETL Pipeline

Execute the full ETL process with:

```bash
python run_pipeline.py
```

This script performs:

```
[1] Extract   → Load raw CSV files
[2] Transform → Clean, normalize, engineer features
[3] Sentiment → Apply BERT model to review text
[4] Load      → Save cleaned CSV + write to SQLite
```

---

## 🧩 ETL Modules Overview

### 🔧 `src/config.py`
Centralized configuration for all file paths:

```python
RAW_MAIN = "data/raw/nordtech_data.csv"
RAW_VAL = "data/raw/nordtech_validation.csv"
CLEANED = "data/processed/nordtech_cleaned.csv"
DB_PATH = "database/nordtech.db"
TABLE_NAME = "clean_orders"
```

---

### 📥 `src/extract.py`
Loads raw datasets:

```python
df_raw = load_main_data()
```

---

### 🧼 `src/transform.py`
Applies all cleaning steps:

- Standardizes column names  
- Cleans IDs  
- Parses mixed date formats  
- Fixes reversed dates  
- Normalizes regions, payment methods, customer types  
- Cleans Swedish number words  
- Cleans prices  
- Cleans ratings  
- Cleans review text  
- Removes duplicates  

Usage:

```python
df_clean = transform_data(df_raw)
```

---

### 💬 `src/sentiment.py`
Adds sentiment using multilingual BERT:

```python
df_clean = add_sentiment_column(df_clean, text_column="recension_text")
```

Sentiment categories:

```
positive
neutral
negative
```

---

### 📤 `src/load.py`
Saves outputs using paths from `config.py`:

```python
save_cleaned_csv(df_clean)
load_to_sqlite(df_clean)
```

---

## 🗄️ Database Output

The cleaned dataset is stored in:

```
database/nordtech.db
```

Table name:

```
clean_orders
```

Example query:

```sql
SELECT region, SUM(total_price)
FROM clean_orders
GROUP BY region;
```

---

## 📊 KPI Analysis

All KPI visualizations are created in:

```
notebooks/03_kpi_analysis.ipynb
```

Figures include:

- Revenue by month  
- Revenue by category  
- Revenue by region  
- Top 10 best sellers  
- Delivery time distribution  
- Rating distribution  
- Sentiment distribution  
- Orders per customer  

---

## 📘 Documentation

- **Data Dictionary** → `reports/data_dictionary.md`  
- **Reflection** → `reports/reflection.pdf`  

---

## 🛠️ Tech Stack

```
Python
pandas
numpy
matplotlib / seaborn
transformers (BERT)
SQLite
Jupyter Notebook
VS Code
```

---

## 🎯 Project Goals

```
✔ Build a professional ETL pipeline
✔ Clean and standardize messy real-world data
✔ Apply NLP sentiment analysis
✔ Generate business-ready KPIs
✔ Produce clear visualizations for presentation
```

---

## 👩‍💻 Author

**Zinah**  
Data Manager Student  
Stockholm, Sweden

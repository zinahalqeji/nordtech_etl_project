# 🧰 Nordtech ETL Pipeline

A complete, modular ETL pipeline for Nordtech's e-commerce dataset.

This project extracts, cleans, transforms, enriches, and loads data into a SQLite database, applies BERT sentiment analysis, and generates business-ready KPIs and visualizations.

The processed data is also presented through a deployed interactive Streamlit dashboard.

---

## 🌐 Live Interactive Dashboard

The results of the ETL pipeline can be explored through an interactive Streamlit application.

👉 **Live Dashboard:** [Nordtech ETL Dashboard](https://nordtechetlproject-sujkvhjnjgr7osud54u3ki.streamlit.app/)

The dashboard includes:

- Interactive filters for category, region, and customer type
- Total revenue, orders, customers, and average rating KPIs
- Revenue analysis by product category
- Customer review sentiment analysis
- Preview of the processed ETL dataset

The dashboard uses the cleaned data produced by the ETL pipeline and provides an interactive way to explore the results.

---

## 📁 Project Structure

```text
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
│   └── load.py
│
├── run_pipeline.py
├── streamlit_app.py
├── requirements.txt
└── README.md
```

---

## 🔄 ETL Workflow

The project follows a modular ETL workflow:

```text
Raw CSV Data
     │
     ▼
   Extract
     │
     ▼
  Transform
     │
     ▼
BERT Sentiment Analysis
     │
     ▼
    Load
   ┌─────┴─────┐
   ▼           ▼
Cleaned CSV   SQLite
                 │
                 ▼
          KPI Analysis
                 │
                 ▼
      Streamlit Dashboard
```

---

## ⚙️ Installation

### 1. Create and activate a virtual environment

```bash
python -m venv venv
```

On Windows:

```bash
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

The pipeline performs:

```text
[1] Extract   → Load raw CSV files
[2] Transform → Clean, normalize, and engineer features
[3] Sentiment → Apply BERT model to review text
[4] Load      → Save cleaned CSV and write to SQLite
```

---

## 🧩 ETL Modules

### 🔧 `src/config.py`

Centralized configuration for file paths and database settings.

```python
RAW_MAIN = "data/raw/nordtech_data.csv"
RAW_VAL = "data/raw/nordtech_validation.csv"
CLEANED = "data/processed/nordtech_cleaned.csv"
DB_PATH = "database/nordtech.db"
TABLE_NAME = "clean_orders"
```

---

### 📥 `src/extract.py`

Responsible for loading the raw datasets used by the pipeline.

Example:

```python
df_raw = load_main_data()
```

---

### 🧼 `src/transform.py`

Handles data cleaning and transformation.

The transformation process includes:

- Standardizing column names
- Cleaning IDs
- Parsing mixed date formats
- Fixing reversed dates
- Normalizing regions
- Normalizing payment methods
- Normalizing customer types
- Cleaning Swedish number words
- Cleaning prices
- Cleaning ratings
- Cleaning review text
- Removing duplicates
- Preparing the dataset for analysis

Example:

```python
df_clean = transform_data(df_raw)
```

---

### 💬 `src/sentiment.py`

Adds sentiment classification to customer reviews using a multilingual BERT model.

Example:

```python
df_clean = add_sentiment_column(df_clean, text_column="recension_text")
```

Sentiment categories:

```text
positive
neutral
negative
```

This enriches the transactional dataset with information that can be used to analyze customer feedback.

---

### 📤 `src/load.py`

Handles the final loading stage of the ETL pipeline.

The processed dataset is saved as a cleaned CSV file and loaded into SQLite.

```python
save_cleaned_csv(df_clean)
load_to_sqlite(df_clean)
```

---

## 🗄️ Database Output

The cleaned dataset is stored in:

```text
database/nordtech.db
```

Table:

```text
clean_orders
```

Example SQL query:

```sql
SELECT region, SUM(total_price)
FROM clean_orders
GROUP BY region;
```

The SQLite database makes it possible to query the transformed data using SQL after the ETL pipeline has completed.

---

## 📊 KPI Analysis

Business-oriented analysis is performed in:

```text
notebooks/03_kpi_analysis.ipynb
```

The analysis includes:

- Revenue by month
- Revenue by category
- Revenue by region
- Top 10 best-selling products
- Delivery time distribution
- Rating distribution
- Sentiment distribution
- Orders per customer

These KPIs provide an analytical view of the cleaned e-commerce data.

---

## 📈 Interactive Streamlit Dashboard

The dashboard is implemented in:

```text
streamlit_app.py
```

Run it locally with:

```bash
streamlit run streamlit_app.py
```

The application provides interactive filtering by:

- Product category
- Region
- Customer type

The dashboard automatically recalculates KPIs and visualizations when filters are changed.

Displayed KPIs include:

```text
Total Revenue
Orders
Customers
Average Rating
```

The application also presents:

- Revenue by category
- Customer review sentiment
- Processed data preview

The application is deployed through Streamlit Community Cloud so the project can be explored directly in a browser.

---

## 📘 Documentation

Additional project documentation is available in:

- **Data Dictionary:** `reports/data_dictionary.md`
- **Reflection:** `reports/reflection.pdf`

---

## 🛠️ Tech Stack

### Data Engineering & Analysis

```text
Python
pandas
NumPy
SQLite
SQL
```

### Machine Learning / NLP

```text
Transformers
Multilingual BERT
PyTorch
```

### Visualization & Application

```text
Streamlit
Matplotlib
Seaborn
```

### Development

```text
Jupyter Notebook
VS Code
Git
GitHub
```

---

## 🎯 Project Goals

The main goals of the project were to:

- ✔ Build a modular ETL pipeline
- ✔ Clean and standardize messy real-world-style data
- ✔ Separate ETL responsibilities into reusable Python modules
- ✔ Store processed data in SQLite
- ✔ Apply NLP sentiment analysis to customer reviews
- ✔ Generate business-oriented KPIs
- ✔ Create clear data visualizations
- ✔ Build an interactive dashboard
- ✔ Deploy the dashboard as a live web application

---

## 👩‍💻 Development

This project was developed independently by me.

I worked on the full data workflow, including:

- Data exploration
- Data cleaning and transformation
- Modular ETL development
- SQLite data loading
- Sentiment analysis
- KPI analysis
- Data visualization
- Streamlit dashboard development
- Deployment of the interactive application

The project demonstrates how raw e-commerce data can be transformed into structured, queryable, and business-ready information and then presented through an interactive application.

---

## 👩‍💻 Author

**Zinah Alqeji**  
Data Management Student  
Stockholm, Sweden
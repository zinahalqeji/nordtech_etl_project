import streamlit as st
import pandas as pd


# --------------------------------------------------
# Page configuration
# --------------------------------------------------

st.set_page_config(page_title="Nordtech ETL Dashboard", page_icon="📊", layout="wide")


# --------------------------------------------------
# Title and description
# --------------------------------------------------

st.title("Nordtech ETL Dashboard")

st.write(
    "Interactive dashboard based on the processed data from my Nordtech ETL pipeline."
)


# --------------------------------------------------
# Load processed data
# --------------------------------------------------


@st.cache_data
def load_data():
    df = pd.read_csv("data/processed/nordtech_cleaned.csv")

    # Calculate revenue for each order line
    df["revenue"] = df["antal"] * df["pris_per_enhet"]

    return df


df = load_data()


# --------------------------------------------------
# Sidebar filters
# --------------------------------------------------

st.sidebar.header("Filters")


# Category filter
categories = sorted(df["kategori"].dropna().unique())

selected_categories = st.sidebar.multiselect("Category", categories, default=categories)


# Region filter
regions = sorted(df["region"].dropna().unique())

selected_regions = st.sidebar.multiselect("Region", regions, default=regions)


# Customer type filter
customer_types = sorted(df["kundtyp"].dropna().unique())

selected_customer_types = st.sidebar.multiselect(
    "Customer Type", customer_types, default=customer_types
)


# --------------------------------------------------
# Apply filters
# --------------------------------------------------

filtered_df = df[
    (df["kategori"].isin(selected_categories))
    & (df["region"].isin(selected_regions))
    & (df["kundtyp"].isin(selected_customer_types))
]


# --------------------------------------------------
# KPI calculations
# --------------------------------------------------

total_revenue = filtered_df["revenue"].sum()

total_orders = filtered_df["order_id"].nunique()

total_customers = filtered_df["kund_id"].nunique()

average_rating = filtered_df["betyg"].mean()


# --------------------------------------------------
# Display KPIs
# --------------------------------------------------

st.subheader("Key Performance Indicators")

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Revenue", f"{total_revenue:,.0f} SEK")

col2.metric("Orders", f"{total_orders:,}")

col3.metric("Customers", f"{total_customers:,}")

if pd.notna(average_rating):
    col4.metric("Average Rating", f"{average_rating:.1f} / 5")
else:
    col4.metric("Average Rating", "N/A")


# --------------------------------------------------
# Revenue by category
# --------------------------------------------------

st.subheader("Revenue by Category")

revenue_by_category = (
    filtered_df.groupby("kategori")["revenue"].sum().sort_values(ascending=False)
)

st.bar_chart(revenue_by_category)


# --------------------------------------------------
# Customer review sentiment
# --------------------------------------------------

st.subheader("Customer Review Sentiment")

sentiment_counts = filtered_df["sentiment_category"].dropna().value_counts()

st.bar_chart(sentiment_counts)


# --------------------------------------------------
# Processed data preview
# --------------------------------------------------

st.subheader("Processed Data")

st.write(f"Number of rows: {len(filtered_df)}")

st.dataframe(filtered_df.head(10), use_container_width=True)

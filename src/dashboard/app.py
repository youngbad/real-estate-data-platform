import os
import pandas as pd
import streamlit as st
import altair as alt
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

# Configure page
st.set_page_config(page_title="Real Estate BI Dashboard", page_icon="🏢", layout="wide")


# =====================================================================
# Database Connection
# =====================================================================
@st.cache_resource
def get_engine():
    """
    Creates a SQLAlchemy engine connected to PostgreSQL.
    Configured via environment variables (with localhost fallback).
    """
    db_user = os.getenv("DB_USER", "postgres")
    db_pass = os.getenv("DB_PASSWORD", "postgres")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "real_estate_db")

    conn_str = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    return create_engine(conn_str)


@st.cache_data(ttl=600)  # Cache data for 10 minutes
def require_data(query: str):
    """Executes a SQL query and returns a pandas DataFrame."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            return pd.read_sql(query, conn)
    except OperationalError as e:
        st.error(
            f"⚠️ Could not connect to PostgreSQL. Ensure it's running and credentials are correct. Error: {e}"
        )
        st.stop()
    except Exception as e:
        st.error(f"⚠️ SQL Execution Error: {e}")
        st.stop()


# =====================================================================
# Layout & UI
# =====================================================================
st.title("🏢 Real Estate Data Platform - BI Dashboard")
st.markdown("Analyzing real estate listings from the PostgreSQL Star Schema.")

# --- Filters ---
st.sidebar.header("Filters")
# Note: For a true dynamic dashboard, you'd pull filter options from the DB.
# We'll leave placeholders here where you can hook up dynamic `WHERE` clauses.
selected_city = st.sidebar.selectbox(
    "Select City", ["All", "Warsaw", "Krakow", "Gdansk", "Wroclaw", "Poznan"]
)

base_where_clause = ""
if selected_city != "All":
    base_where_clause = f"WHERE l.city = '{selected_city}'"

# =====================================================================
# Top Level KPIs
# =====================================================================
kpi_query = f"""
    SELECT 
        COUNT(f.listing_sk) AS total_listings,
        ROUND(AVG(f.price_per_m2), 2) AS avg_price_m2,
        ROUND(AVG(f.price), 2) AS avg_total_price
    FROM fact_listings f
    JOIN dim_location l ON f.location_id = l.location_id
    {base_where_clause}
"""

kpi_data = require_data(kpi_query)

if not kpi_data.empty:
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Listings", f"{kpi_data['total_listings'][0]:,}")
    col2.metric("Avg Price per m²", f"{kpi_data['avg_price_m2'][0]:,} PLN")
    col3.metric("Avg Total Price", f"{kpi_data['avg_total_price'][0]:,} PLN")

st.markdown("---")

# =====================================================================
# Visualizations
# =====================================================================
colA, colB = st.columns(2)

# --- 1. Trends over time ---
with colA:
    st.subheader("Price Trends Over Time")
    trend_query = f"""
        SELECT 
            d.year, 
            d.month,
            d.month_name,
            ROUND(AVG(f.price_per_m2), 2) AS avg_price_m2
        FROM fact_listings f
        JOIN dim_date d ON f.date_id = d.date_id
        JOIN dim_location l ON f.location_id = l.location_id
        {base_where_clause}
        GROUP BY d.year, d.month, d.month_name
        ORDER BY d.year, d.month
    """

    trend_df = require_data(trend_query)

    if not trend_df.empty:
        # Create a display column for time
        trend_df["Year-Month"] = (
            trend_df["year"].astype(str)
            + "-"
            + trend_df["month"].astype(str).str.zfill(2)
        )

        chart_trend = (
            alt.Chart(trend_df)
            .mark_line(point=True)
            .encode(
                x=alt.X("Year-Month:T", title="Date"),
                y=alt.Y(
                    "avg_price_m2:Q",
                    title="Avg Price per m² (PLN)",
                    scale=alt.Scale(zero=False),
                ),
                tooltip=["Year-Month", "avg_price_m2"],
            )
            .properties(height=350)
        )

        st.altair_chart(chart_trend, use_container_width=True)
    else:
        st.info("No data available for trends.")


# --- 2. Analysis by City/District ---
with colB:
    st.subheader("Average Price by Location")
    loc_query = f"""
        SELECT 
            l.city,
            ROUND(AVG(f.price_per_m2), 2) AS avg_price_m2
        FROM fact_listings f
        JOIN dim_location l ON f.location_id = l.location_id
        {base_where_clause}
        GROUP BY l.city
        ORDER BY avg_price_m2 DESC
    """

    loc_df = require_data(loc_query)

    if not loc_df.empty:
        chart_loc = (
            alt.Chart(loc_df)
            .mark_bar(color="#4c78a8")
            .encode(
                x=alt.X("city:N", sort="-y", title="City"),
                y=alt.Y("avg_price_m2:Q", title="Avg Price per m² (PLN)"),
                tooltip=["city", "avg_price_m2"],
            )
            .properties(height=350)
        )

        st.altair_chart(chart_loc, use_container_width=True)
    else:
        st.info("No data available for locations.")


st.markdown("---")

colC, colD = st.columns(2)

# --- 3. Analysis by Property Size (Area Bucket) ---
with colC:
    st.subheader("Price by Area Category")
    area_query = f"""
        SELECT 
            p.area_bucket,
            ROUND(AVG(f.price_per_m2), 2) AS avg_price_m2
        FROM fact_listings f
        JOIN dim_property p ON f.property_id = p.property_id
        JOIN dim_location l ON f.location_id = l.location_id
        {base_where_clause}
        GROUP BY p.area_bucket
        ORDER BY p.area_bucket
    """

    area_df = require_data(area_query)

    if not area_df.empty:
        chart_area = (
            alt.Chart(area_df)
            .mark_bar(color="#f58518")
            .encode(
                x=alt.X("area_bucket:N", title="Area Size (m²)"),
                y=alt.Y("avg_price_m2:Q", title="Avg Price per m² (PLN)"),
                tooltip=["area_bucket", "avg_price_m2"],
            )
            .properties(height=300)
        )
        st.altair_chart(chart_area, use_container_width=True)


# --- 4. Analysis by Number of Rooms ---
with colD:
    st.subheader("Price by Room Count")
    room_query = f"""
        SELECT 
            p.rooms,
            ROUND(AVG(f.price_per_m2), 2) AS avg_price_m2
        FROM fact_listings f
        JOIN dim_property p ON f.property_id = p.property_id
        JOIN dim_location l ON f.location_id = l.location_id
        {base_where_clause}
        WHERE p.rooms IS NOT NULL
        GROUP BY p.rooms
        ORDER BY p.rooms
    """

    room_df = require_data(room_query)

    if not room_df.empty:
        chart_room = (
            alt.Chart(room_df)
            .mark_bar(color="#e45756")
            .encode(
                x=alt.X("rooms:O", title="Number of Rooms"),
                y=alt.Y("avg_price_m2:Q", title="Avg Price per m² (PLN)"),
                tooltip=["rooms", "avg_price_m2"],
            )
            .properties(height=300)
        )
        st.altair_chart(chart_room, use_container_width=True)

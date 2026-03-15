# 🏙️ Real Estate Data Platform

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Apache PySpark](https://img.shields.io/badge/PySpark-3.5.0-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15%2B-316192?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.32.0-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)

A modern end-to-end Data Engineering pipeline designed to ingest, process, and analyze real estate market data. This project showcases ability to design scalable ETL workflows, process big data utilizing Apache Spark, manage data warehouses with PostgreSQL, and build interactive BI dashboard applications.

---

## 🏗️ Architecture & Workflow

The pipeline is structured into distinct layers corresponding to industry-standard data engineering practices (Medallion Architecture: Raw -> Processed -> Curated):

1. **Ingestion Layer (Scraping):** Automated Python scrapers collect real estate listings from multiple sources (e.g., OLX, Otodom, GUS) and save the raw data in JSON format within the `data/raw` zone.
2. **Processing Layer (PySpark Transformation):** Apache Spark processes the raw unstructured JSON. It applies data cleaning, normalizes schemas, and performs type conversions. The output is stored in optimized columnar format (Parquet) under `data/processed` and `data/curated`.
3. **Storage Layer (Data Warehouse):** Curated Parquet data is loaded into a relational PostgreSQL database modeling the properties and market trends.
4. **Presentation Layer (Dashboard):** A Streamlit application provides an interactive BI interface to visualize housing market trends, average prices, and geospatial data insights.

## 🛠️ Tech Stack

- **Languages:** Python, SQL
- **Data Ingestion/Web Scraping:** `requests`, `BeatifulSoup4`
- **Data Processing (ETL/ELT):** `PySpark`, `Pandas`
- **Database / Storage:** `PostgreSQL`, `SQLAlchemy`, `psycopg2`
- **Data Visualization (BI):** `Streamlit`, `Altair`
- **Orchestration:** `Apache Airflow`

---

## 📂 Project Structure

```text
real-estate-data-platform/
├── dags/                  # Apache Airflow DAG definitions
│   └── real_estate_etl.py # Daily ETL orchestration DAG
├── data/                  # Local Data Lake (Medallion architecture)
│   ├── raw/               # Landing zone for JSON files
│   ├── processed/         # Cleaned Parquet files
│   └── curated/           # Business-level aggregated Parquet data
├── sql/                   # Database scripts
│   └── schema.sql         # PostgreSQL schema definition
├── src/                   # Source code
│   ├── dashboard/         # Streamlit BI application
│   ├── ingestion/         # Scrapers and API data loaders
│   ├── jobs/              # Main execution scripts (init, load, process)
│   └── processing/        # PySpark ETL transformations
├── requirements.txt       # Project dependencies
└── README.md              # You are here!
```

---

## 🚀 Getting Started

### Prerequisites
- Python 3.9+
- Java 11+ (Required for Apache Spark)
- PostgreSQL Server up and running

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/real-estate-data-platform.git
   cd real-estate-data-platform
   ```

2. **Set up a virtual environment and install dependencies:**
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Initialize the Database Schema:**
   *(Ensure you have configured your local Postgres credentials inside the script)*
   ```bash
   python src/jobs/init_db.py
   ```

### Running the Pipeline

1. **Scrape Raw Data:**
   *(Optional if raw data is already present in `data/raw`)*
   Run the specific scrapers or json loader to pull fresh data.
   
2. **Process and Transform (ETL):**
   Execute the PySpark job to process raw JSON into curated Parquet datasets:
   ```bash
   python src/jobs/process_listings.py
   ```

3. **Load to Data Warehouse:**
   Push the curated Parquet data into Postgres:
   ```bash
   python src/jobs/load_to_postgres.py
   ```

4. **Launch the Dashboard:**
   Start the Streamlit analytics app:
   ```bash
   streamlit run src/dashboard/app.py
   ```

---

## ⚙️ Orchestration with Apache Airflow

The pipeline is fully automated using **Apache Airflow**. The DAG definition lives in `dags/real_estate_etl.py`.

### DAG: `real_estate_etl_pipeline`

| Property | Value |
|---|---|
| **Schedule** | Daily (`@daily`) |
| **Start Date** | 2026-03-15 |
| **Catchup** | Disabled |
| **Tags** | `real_estate`, `etl` |
| **Retries** | 1 (5-minute retry delay) |

### Task Graph

```
initialize_database → scrape_data → transform_data → load_to_postgres
```

| Task ID | Script | Description |
|---|---|---|
| `initialize_database` | `src/jobs/init_db.py` | Creates PostgreSQL tables (idempotent) |
| `scrape_data` | `src/ingestion/scraper.py` | Scrapes raw listings and saves JSON to `data/raw/` |
| `transform_data` | `src/processing/transformer.py` | PySpark ETL: cleans JSON → Parquet in `data/processed/` & `data/curated/` |
| `load_to_postgres` | `src/jobs/load_to_postgres.py` | Loads curated Parquet data into PostgreSQL |

### Running Airflow Locally

1. **Set the project root** in `dags/real_estate_etl.py` (update `PROJECT_ROOT` to your local path).

2. **Initialize the Airflow metadata database** (first time only):
   ```bash
   airflow db init
   ```

3. **Start the Airflow webserver and scheduler** (in separate terminals):
   ```bash
   airflow webserver --port 8080
   airflow scheduler
   ```

4. **Open the Airflow UI** at [http://localhost:8080](http://localhost:8080), find the `real_estate_etl_pipeline` DAG, and enable it to begin scheduled runs (or trigger it manually).

---

## 🔮 Future Enhancements

- [x] Implement **Apache Airflow** DAGs to automate the daily ETL processes.
- [ ] Migrate data lake storage from local file system to **AWS S3** or **GCS**.
- [ ] Integrate **dbt (data build tool)** for advanced SQL transformations in the data warehouse.
- [ ] Containerize the entire stack using **Docker** and `docker-compose`.

---

*If you found this project interesting or have any questions about the data engineering architecture, feel free to reach out!*

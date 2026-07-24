

# 🍽️ Restaurant Analytics Lakehouse on Databricks

An end-to-end **Data Engineering and Analytics Lakehouse** built using Databricks, Apache Spark, PySpark, Spark SQL, Delta Lake, and Unity Catalog.

The project implements a complete **Medallion Architecture (Bronze → Silver → Gold)** for restaurant operational data, performs data quality validation, orchestrates the complete pipeline using Databricks Jobs, and exposes business insights through an interactive Databricks SQL dashboard.

The project is version-controlled using Git/GitHub and supports reproducible deployment using **Databricks Declarative Automation Bundles**.

---

## 📌 Project Overview

Restaurant platforms generate data across multiple operational systems, including:

- Customers
- Restaurants
- Menu items
- Orders
- Order items
- Customer reviews

This project demonstrates how raw restaurant data can be transformed into analytics-ready datasets using a modern Lakehouse architecture.

The complete pipeline follows:

```text
Synthetic Restaurant Data
          ↓
CSV Files
          ↓
Unity Catalog Volume
          ↓
Bronze Delta Tables
          ↓
Silver Transformations
          ↓
Gold Analytics Layer
         ↙ ↘
SQL Dashboard   Data Quality
```

---

## 🏗️ Architecture

![Restaurant Lakehouse Architecture](screenshots/architecture.png)

The implementation follows the Databricks **Medallion Architecture**.

### Bronze Layer

Raw source data is ingested from a Unity Catalog Volume and stored as Delta tables with minimal transformation.

### Silver Layer

Bronze data is cleaned, standardized, validated, and transformed into analytics-ready datasets.

### Gold Layer

Business-level aggregations are generated for reporting, KPI calculation, and dashboard visualization.

### Serving Layer

Gold datasets are consumed by the Databricks SQL dashboard for business analytics.

---

## 🛠️ Tech Stack

| Technology | Purpose |
|---|---|
| Databricks | Lakehouse platform |
| Apache Spark | Distributed data processing |
| PySpark | Data transformation |
| Spark SQL | SQL transformations and analytics |
| Delta Lake | Reliable Lakehouse storage |
| Unity Catalog | Data organization and governance |
| Databricks Volumes | Raw data storage |
| Databricks Jobs | Pipeline orchestration |
| Databricks SQL | Analytics and dashboarding |
| Declarative Automation Bundles | Deployment as code |
| Python | Data engineering logic |
| SQL | Analytics and transformations |
| Git | Version control |
| GitHub | Source-code hosting |

---

## 📂 Source Datasets

The project uses five primary restaurant datasets.

| Dataset | Description |
|---|---|
| `customers.csv` | Customer profiles and registration information |
| `restaurants.csv` | Restaurant locations and metadata |
| `menu_items.csv` | Menu items, categories, prices and ingredients |
| `historical_orders.csv` | Historical restaurant transactions |
| `customer_reviews.csv` | Customer ratings and reviews |

The files are uploaded into:

```text
/Volumes/restaurant_lakehouse/landing/restaurant_files/
```

Example:

```text
restaurant_files/
│
├── customers.csv
├── restaurants.csv
├── menu_items.csv
├── historical_orders.csv
└── customer_reviews.csv
```

---

# 🥉 Bronze Layer

The Bronze layer contains the raw datasets ingested from the Unity Catalog Volume.

![Bronze Layer](screenshots/bronze-catalog.png)

The primary Bronze tables include:

```text
restaurant_lakehouse.bronze.customers
restaurant_lakehouse.bronze.restaurants
restaurant_lakehouse.bronze.menu_items
restaurant_lakehouse.bronze.historical_orders
restaurant_lakehouse.bronze.reviews
```

The Bronze ingestion pipeline handles:

- CSV ingestion
- Header detection
- Schema inference
- Data-type preservation
- Quoted CSV fields
- Nested order-item content
- Delta table creation

Historical orders require careful CSV parsing because the `items` field contains nested structured data.

Example ingestion:

```python
historical_orders_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", "true")
    .option("mode", "PERMISSIVE")
    .csv(
        "/Volumes/restaurant_lakehouse/landing/restaurant_files/historical_orders.csv"
    )
)
```

The resulting DataFrame is persisted as a Delta table.

---

# 🥈 Silver Layer

The Silver layer transforms the raw datasets into cleaned and analytics-ready structures.

![Silver Layer](screenshots/silver-catalog.png)

Silver transformations include:

- Data-type standardization
- Null handling
- Malformed-value handling
- Order amount cleaning
- Timestamp processing
- Nested JSON parsing
- Order-item explosion
- Fact-table preparation
- Dimension-style transformations

One of the key engineering challenges in the project was processing the nested `items` field contained inside historical orders.

Example structure:

```json
[
  {
    "item_id": "ITEM-103",
    "name": "Chicken 65",
    "category": "Starter",
    "quantity": 2,
    "unit_price": 38.24,
    "subtotal": 76.48
  }
]
```

The field is converted from a string representation into structured Spark data and exploded into individual order-item records.

This enables item-level analytics that would not be possible directly from the raw order record.

---

# 🥇 Gold Analytics Layer

The Gold layer contains business-level datasets optimized for reporting and analytics.

![Gold Layer](screenshots/gold-catalog.png)

The Gold layer provides metrics such as:

- Total revenue
- Daily revenue
- Total orders
- Unique customers
- Average order value
- Restaurant performance
- Menu-item performance
- Revenue trends

For example, daily sales analytics can be generated from:

```sql
SELECT
    order_date,
    ROUND(SUM(total_revenue), 2) AS revenue
FROM restaurant_lakehouse.gold.d_sales_summary
GROUP BY order_date
ORDER BY order_date;
```

The Gold layer acts as the serving layer for the analytics dashboard.

---

# 📊 Restaurant Analytics Dashboard

![Restaurant Analytics Dashboard](screenshots/dashboard.png)

An interactive analytics dashboard was created using Databricks SQL.

The dashboard provides visibility into important restaurant KPIs and business trends.

Key metrics include:

- Total Revenue
- Total Orders
- Average Order Value
- Unique Customers
- Revenue Trend
- Restaurant Performance
- Customer Activity

### Revenue Trend

Daily revenue is calculated using the Gold sales summary.

```sql
SELECT
    order_date,
    ROUND(SUM(total_revenue), 2) AS revenue
FROM restaurant_lakehouse.gold.d_sales_summary
GROUP BY order_date
ORDER BY order_date;
```

This allows business users to identify changes in restaurant revenue over time.

---

# ✅ Data Quality Validation

The final pipeline stage performs automated data-quality checks before the datasets are consumed for analytics.

![Data Quality Validation](screenshots/data-quality.png)

Example checks include:

- Silver fact table contains records
- Order IDs contain no NULL values
- Order IDs are unique
- Bronze customer data exists
- Bronze restaurant data exists
- Gold analytics tables contain records

Example validation logic:

```python
orders = spark.table(
    "restaurant_lakehouse.silver.fact_orders"
)

checks.append(
    ("Silver fact_orders contains records", orders.count() > 0)
)

checks.append(
    (
        "Order IDs contain no NULL values",
        orders.filter(
            F.col("order_id").isNull()
        ).count() == 0
    )
)

checks.append(
    (
        "Order IDs are unique",
        orders.select("order_id").distinct().count()
        == orders.count()
    )
)
```

The results are displayed as a simple PASS/FAIL quality report.

---

# ⚙️ Pipeline Orchestration

The complete Lakehouse pipeline is orchestrated using Databricks Jobs.

![Databricks Pipeline Execution](screenshots/job-run.png)

The execution dependency is:

```text
┌──────────────────────────┐
│     Bronze Ingestion     │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│  Silver Transformations  │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│      Gold Analytics      │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│ Data Quality Validation  │
└──────────────────────────┘
```

Task dependencies ensure that downstream processing only begins after the required upstream task completes successfully.

This provides reproducible end-to-end pipeline execution rather than manually running notebooks in an arbitrary order.

---

# 🚀 Deployment

The project uses **Databricks Declarative Automation Bundles** to define and deploy the Databricks workflow as code.

The deployment configuration is stored alongside the application code in GitHub.

```text
GitHub Repository
       │
       ▼
databricks.yml
       │
       ▼
resources/restaurant_job.yml
       │
       ▼
Databricks Workspace
       │
       ▼
Restaurant Lakehouse Pipeline
```

## Validate

Before deployment:

```bash
databricks bundle validate -t dev --profile restaurant-dev
```

## Deploy

Deploy the bundle to Databricks:

```bash
databricks bundle deploy -t dev --profile restaurant-dev
```

## Run

Execute the deployed pipeline:

```bash
databricks bundle run -t dev restaurant_lakehouse_job --profile restaurant-dev
```

The deployment process therefore follows:

```text
Local Repository
       │
       ▼
Bundle Validation
       │
       ▼
Bundle Deployment
       │
       ▼
Databricks Job
       │
       ▼
Bronze
       ↓
Silver
       ↓
Gold
       ↓
Data Quality
```

---

# 📁 Project Structure

```text
restaurant-databricks-lakehouse/
│
├── 00_synthetic_data/
│   └── ...
│
├── notebooks/
│   ├── 01_bronze_ingestion.ipynb
│   ├── 02_silver_transformations.ipynb
│   ├── 03_gold_analytics.ipynb
│   └── 04_data_quality_validation.ipynb
│
├── resources/
│   └── restaurant_job.yml
│
├── screenshots/
│   ├── architecture.png
│   ├── bronze-catalog.png
│   ├── silver-catalog.png
│   ├── gold-catalog.png
│   ├── dashboard.png
│   ├── job-run.png
│   └── data-quality.png
│
├── docs/
│   └── ...
│
├── databricks.yml
├── README.md
└── .gitignore
```

---

# 🔄 End-to-End Data Flow

```text
                    Restaurant Data
                          │
                          ▼
                ┌───────────────────┐
                │   CSV Data Files  │
                └─────────┬─────────┘
                          │
                          ▼
                ┌───────────────────┐
                │   Unity Catalog   │
                │      Volume       │
                └─────────┬─────────┘
                          │
                          ▼
                ┌───────────────────┐
                │      BRONZE       │
                │ Raw Delta Tables  │
                └─────────┬─────────┘
                          │
                          ▼
                ┌───────────────────┐
                │      SILVER       │
                │ Clean + Transform │
                └─────────┬─────────┘
                          │
                          ▼
                ┌───────────────────┐
                │       GOLD        │
                │ Business Metrics  │
                └─────────┬─────────┘
                          │
                   ┌──────┴──────┐
                   │             │
                   ▼             ▼
             ┌───────────┐ ┌──────────────┐
             │ Dashboard │ │ Data Quality │
             └───────────┘ └──────────────┘
```

---

# 💡 Business Questions Answered

The analytics layer can answer questions such as:

1. How much total revenue has the restaurant platform generated?
2. How is revenue changing over time?
3. How many orders are being processed?
4. What is the average order value?
5. Which restaurants generate the most revenue?
6. How many unique customers are ordering?
7. Which menu items contribute most to sales?
8. How does restaurant performance vary across locations?

---

# 🔑 Key Engineering Features

- End-to-end Databricks Lakehouse implementation
- Bronze, Silver and Gold Medallion Architecture
- Apache Spark distributed processing
- PySpark-based ETL transformations
- Spark SQL analytics
- Delta Lake tables
- Unity Catalog integration
- Unity Catalog Volume-based ingestion
- Nested JSON processing
- Order-item explosion
- Fact-oriented data modeling
- Gold business aggregations
- Automated data-quality validation
- Databricks Job orchestration
- Task dependency management
- Serverless execution
- Interactive Databricks SQL dashboard
- Databricks CLI deployment
- Declarative Automation Bundles
- Git/GitHub version control

---

# 🧩 Challenges Solved

Several practical data-engineering problems were addressed while implementing the pipeline.

### Nested Order Items

Historical order data contained nested item information embedded inside quoted CSV fields.

The ingestion logic was configured to correctly preserve these values before parsing them in the Silver layer.

### Malformed Numeric Values

Potential malformed values were handled during transformation rather than allowing individual records to break the entire pipeline.

### Notebook Dependency Management

Each pipeline notebook was made independently executable so Databricks Jobs could run them in fresh execution contexts without relying on variables from previous interactive notebook sessions.

### Pipeline Reproducibility

The Databricks Job was represented as deployment configuration so the workflow can be validated and deployed from source control.

---

# ▶️ Running the Project

## Prerequisites

You need:

- Databricks workspace
- Databricks CLI
- Git
- GitHub
- Access to Unity Catalog
- Serverless compute or compatible Databricks compute

Clone the repository:

```bash
git clone https://github.com/arsal-nez/restaurant-databricks-lakehouse.git
```

Enter the project:

```bash
cd restaurant-databricks-lakehouse
```

Authenticate the Databricks CLI:

```bash
databricks auth login --host <YOUR-DATABRICKS-WORKSPACE-URL> --profile restaurant-dev
```

Verify authentication:

```bash
databricks current-user me --profile restaurant-dev
```

Validate the deployment:

```bash
databricks bundle validate -t dev --profile restaurant-dev
```

Deploy:

```bash
databricks bundle deploy -t dev --profile restaurant-dev
```

Run:

```bash
databricks bundle run -t dev restaurant_lakehouse_job --profile restaurant-dev
```

---

# 📈 Project Outcome

This project demonstrates the complete lifecycle of a modern analytics pipeline:

```text
Data Generation
      ↓
Data Ingestion
      ↓
Lakehouse Storage
      ↓
Data Transformation
      ↓
Data Modeling
      ↓
Business Aggregation
      ↓
Data Quality
      ↓
Pipeline Orchestration
      ↓
Analytics Dashboard
      ↓
Deployment as Code
```

It demonstrates practical experience with data ingestion, distributed transformations, Delta Lake, Medallion Architecture, pipeline orchestration, analytics, data-quality validation, source control, and Databricks deployment.

---

# 👤 Author

**Md Arsalan**

B.Tech, Chemical Engineering  
National Institute of Technology Agartala

Interested in Data Analytics, Data Engineering, Data Science and Software Engineering.

---

## ⭐ Repository

If you found this project useful, consider starring the repository.
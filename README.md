# 🛒 ShopStream — Databricks Data Engineering Project

ShopStream is an end-to-end **Data Engineering project** for an e-commerce company that sells products ranging from mechanical keyboards to yoga mats.

The project demonstrates how to build a modern **Lakehouse data pipeline** using Databricks, starting from raw data ingestion and ending with business-ready analytics and dashboards.

---

## 📌 Project Overview

ShopStream has two main data challenges:

1. **Batch data** — six months of historical customers, products, and orders stored as CSV files.
2. **Streaming data** — new order events arriving continuously as JSON events.

The project processes both types of data using a **Medallion Architecture**:

```text
                    Raw Data
                       │
              ┌────────┴────────┐
              │                 │
          CSV Files         JSON Events
              │                 │
              ▼                 ▼
           Bronze           Auto Loader
              │                 │
              └────────┬────────┘
                       ▼
                    Silver
                       │
                       ▼
                     Gold
                       │
                       ▼
                  Dashboard
```

---

# 🏗️ Architecture

## 🥉 Bronze Layer

The Bronze layer is responsible for ingesting raw data into Databricks with minimal transformation.

Historical data includes:

- Customers
- Products
- Orders

The data is stored as **Delta tables**.

Example:

```text
Raw CSV
   ↓
Databricks Volume
   ↓
COPY INTO
   ↓
Bronze Delta Table
```

Streaming order events are ingested continuously via **Auto Loader**, landing new JSON events into the same unified `orders` Bronze table alongside the historical batch rows.

---

## 🥈 Silver Layer

The Silver layer contains cleaned and prepared data.

Transformations include:

- Data type standardization
- Data cleaning
- Handling invalid records
- Removing duplicates where appropriate
- Handling missing values
- Preparing data for analytical use

Example:

```text
Bronze
   ↓
Cleaning & Transformation
   ↓
Silver
```

Because the Bronze `orders` table contains rows from two different sources with different identifier schemes, Silver reconciles them into one canonical schema — see **Engineering Notes** below.

---

## 🥇 Gold Layer

The Gold layer contains business-ready datasets and metrics designed for analytics and dashboards.

Examples of metrics include:

- Total revenue
- Units sold
- Gross profit
- Gross margin
- Category performance
- Daily revenue
- Revenue by country
- Revenue by signup channel
- Customer lifetime value

Example:

```text
Silver
   ↓
Business Transformations
   ↓
Gold Tables
   ↓
Dashboard
```

---

# 📊 Dashboard

The project includes a Databricks dashboard for monitoring and analyzing ShopStream's business performance.

## Current Dashboard
<img width="1254" height="612" alt="dashboard" src="https://github.com/user-attachments/assets/b3e0082c-e20d-4628-a29b-11121be905f1" />

### Current visualizations

- **Gross Margin by Category**
- **Daily Revenue Trend**
- **Revenue Distribution by Country**
- **Revenue by Signup Channel**

These visualizations help analyze business performance across:

- Product categories
- Time
- Countries
- Customer acquisition channels

---

# 🛠️ Technology Stack

| Technology | Purpose |
|---|---|
| **Databricks Free Edition** | Lakehouse platform |
| **Apache Spark** | Distributed data processing |
| **PySpark** | Data transformation |
| **Spark SQL** | SQL-based analytics |
| **Delta Lake** | Reliable table storage |
| **Unity Catalog** | Data organization and governance |
| **Auto Loader** | Streaming file ingestion |
| **SQL** | Data analysis and transformation |
| **Databricks Dashboards** | Business analytics |
| **Databricks Lakeflow Jobs** | Orchestration and task scheduling |
| **Git & GitHub** | Version control |

---

# 🔄 Data Pipeline

The overall pipeline follows:

```text
                   DATA SOURCES
                       │
          ┌────────────┴────────────┐
          │                         │
      Historical CSV            JSON Events
          │                         │
          ▼                         ▼
       BATCH                    STREAMING
          │                         │
          └────────────┬────────────┘
                       │
                       ▼
                  🥉 BRONZE
                       │
                       ▼
                   🥈 SILVER
                       │
                       ▼
                    🥇 GOLD
                       │
                       ▼
                  DASHBOARD
```

---

# 📈 Business Metrics

The Gold layer provides business metrics that can be used by analysts and business stakeholders.

### Revenue

```text
Revenue = Quantity × Unit Price
```

### Cost

```text
Cost = Quantity × Unit Cost
```

### Gross Profit

```text
Gross Profit = Revenue − Cost
```

### Gross Margin

```text
Gross Margin % =
(Gross Profit / Revenue) × 100
```

### Category Performance

Category performance can be analyzed using:

- Revenue
- Units sold
- Orders
- Gross profit
- Gross margin

Categories are obtained from the **Product Dimension** rather than duplicating category information inside the fact table.

### Customer Lifetime Value

Historical customer lifetime value is calculated from the total value generated by a customer across their completed orders.

```text
Customer Lifetime Value =
Total Customer Revenue
```

---

# 🧱 Data Modeling

The analytical model follows dimensional modeling principles.

A simplified structure is:

```text
                 dim_customer
                      │
                      │
                      ▼
dim_date ───────► fact_order_items ◄────── dim_product
                      │
                      │
                      ├── order_id
                      ├── status
                      ├── coupon_code
                      ├── quantity
                      ├── unit_price
                      ├── unit_cost
                      ├── sales_amount
                      └── cost_amount
```

The **Product Dimension** contains attributes such as:

```text
product_id
product_name
category
unit_price
unit_cost
```

The fact table contains transactional measurements such as:

```text
quantity
sales_amount
cost_amount
```

This allows business metrics to be calculated at different levels of analysis.

---

# 🚀 Streaming Pipeline

The project also includes a streaming ingestion path for continuously arriving JSON order events.

The streaming architecture is:

```text
JSON Events
     │
     ▼
Auto Loader
     │
     ▼
Bronze Delta Table
     │
     ▼
Silver
     │
     ▼
Gold
     │
     ▼
Dashboard
```

Auto Loader allows new files to be detected and processed incrementally as they arrive. Streaming order events land in the same Bronze `orders` table as the historical batch data, and are reconciled into a single canonical schema during Silver processing.

---

# 🧩 Engineering Notes

One integration challenge in this project: historical (batch) orders and streaming order events use **different identifier schemes**. Batch rows carry a real `order_line_id`, while streaming events instead carry an `event_id` (with `order_line_id` left null) and use a separate `event_ts` field instead of `order_ts`.

The Silver layer reconciles this by:

- Coalescing `order_line_id` and `event_id` into one canonical `order_line_id`
- Coalescing `order_ts` and `event_ts` into one canonical `order_timestamp`
- Tagging each row with a `source_system` column (`batch` or `streaming`) for lineage and debugging

This lets downstream Silver and Gold logic treat both sources identically, without needing to know which pipeline a given row originally came from.

---

# ⚙️ Production Workflow

The production pipeline is orchestrated as a **Databricks Lakeflow Job** (`shop_stream_job`) with explicit task dependencies, chaining notebooks into a single scheduled DAG on serverless compute:

```text
                         ┌──► customer_processing ──┐
                         │                           │
load_historical_data ────┼──► products_processing ───┼──► gold_processing
                         │                           │
streaming_data ──────────┴──► order_processing ──────┘
```

- **`load_historical_data`** and **`streaming_data`** run as independent root tasks — one loads the historical CSVs into Bronze, the other runs Auto Loader against the incoming JSON order events.
- **`order_processing`** depends on **both** root tasks, since the Bronze `orders` table is fed by both the batch load and the streaming feed.
- **`customer_processing`** and **`products_processing`** depend only on `load_historical_data`, since those dimensions have no streaming source.
- **`gold_processing`** depends on all three Silver tasks, ensuring Gold always builds from fully refreshed data.

All tasks run on **serverless** compute.
<img width="970" height="540" alt="image" src="https://github.com/user-attachments/assets/5737e42f-69af-42cc-bb6e-6b95b013798f" />

---

# 🎯 Project Goals

The main goal of this project is to build a complete Data Engineering pipeline rather than working with isolated technologies.

The project covers:

- [x] Batch data ingestion
- [x] Bronze layer
- [x] Silver layer
- [x] Gold layer
- [x] Delta tables
- [x] PySpark
- [x] Spark SQL
- [x] Dimensional modeling
- [x] Business metrics
- [x] Databricks dashboard
- [x] Streaming ingestion with Auto Loader
- [x] Batch + streaming integration
- [ ] Declarative Lakeflow Pipeline (DLT)
- [x] Data quality checks
- [x] Production Databricks Lakeflow Job
- [x] Task dependencies
- [ ] Pipeline monitoring

---

# 📚 Key Learning Outcomes

This project connects several Data Engineering concepts into one practical workflow:

```text
Python
  ↓
SQL
  ↓
PySpark
  ↓
Delta Lake
  ↓
Databricks
  ↓
Medallion Architecture
  ↓
Data Modeling
  ↓
Batch Processing
  ↓
Streaming
  ↓
Gold Analytics
  ↓
Dashboard
  ↓
Production Pipeline
```

The project is designed to demonstrate practical experience with building and managing a modern Lakehouse pipeline.

---

# 🔮 Future Improvements

Planned improvements include:

- Implement a Lakeflow Declarative Pipeline (DLT) version of Bronze → Silver → Gold
- Add pipeline monitoring and failure alerting to the Lakeflow Job
- Improve Gold-layer analytics
- Add additional customer analytics
- Add more dashboard KPIs

---

# 👨‍💻 Author

**Youssef Alaraby**

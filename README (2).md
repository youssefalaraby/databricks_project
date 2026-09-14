# ShopStream — Databricks Data Engineering Project

ShopStream is an end-to-end data engineering project for an e-commerce company. The project demonstrates how raw business data can be ingested, cleaned, transformed, modeled, and prepared for analytics using **Databricks** and a **Medallion Architecture**.

## Project Overview

ShopStream has two main data sources:

- Historical e-commerce data stored as CSV files
- New order events that can arrive continuously as JSON events

The pipeline is designed around the Bronze → Silver → Gold architecture.

```text
                    Raw Data
                       │
             ┌─────────┴─────────┐
             │                   │
          CSV Files          JSON Events
             │                   │
             ▼                   ▼
          Bronze             Auto Loader
             │                   │
             └─────────┬─────────┘
                       ▼
                    Silver
                       │
                       ▼
                     Gold
                       │
                       ▼
                  Dashboard
```

## Architecture

### Bronze Layer

The Bronze layer contains data ingested from the raw source with minimal transformation.

Example sources:

- `customers.csv`
- `products.csv`
- `orders.csv`

The data is stored as **Delta tables** in Databricks.

### Silver Layer

The Silver layer contains cleaned and prepared data.

Typical transformations include:

- Data type standardization
- Cleaning and validation
- Handling invalid records
- Removing duplicates where appropriate
- Preparing data for analytical modeling

### Gold Layer

The Gold layer contains business-ready datasets and metrics used by analytics and dashboards.

Examples of analytical metrics include:

- Revenue
- Units sold
- Gross profit
- Gross margin
- Category performance
- Daily revenue
- Revenue by country
- Revenue by signup channel
- Customer lifetime value

## Dashboard

The project includes a Databricks dashboard for analyzing ShopStream's business performance.

### Current Dashboard

![ShopStream Dashboard](dashboard.png)

The dashboard currently includes visualizations such as:

- **Gross Margin by Category**
- **Daily Revenue Trend**
- **Revenue Distribution by Country**
- **Revenue by Signup Channel**

These visualizations allow business users to understand sales performance across products, time, countries, and acquisition channels.

## Technology Stack

- **Databricks Free Edition**
- **Apache Spark / PySpark**
- **Spark SQL**
- **Delta Lake**
- **Unity Catalog**
- **Auto Loader** (streaming ingestion)
- **SQL**
- **Databricks Dashboards**
- **Git / GitHub**

## Data Pipeline

The general processing flow is:

```text
Raw CSV / JSON
      ↓
   Bronze
      ↓
   Silver
      ↓
    Gold
      ↓
 Dashboard / Analytics
```

The project uses Delta tables to provide a reliable storage layer between the different stages of the pipeline.

## Project Goals

This project is designed to demonstrate practical Data Engineering skills, including:

1. Batch data ingestion
2. Lakehouse architecture
3. Medallion architecture
4. Data cleaning and transformation
5. Delta Lake tables
6. PySpark and Spark SQL
7. Analytical data modeling
8. Business metric development
9. Dashboard development
10. Streaming ingestion with Auto Loader
11. Production orchestration with Databricks Jobs

## Future Improvements

Planned additions to the project include:

- Continuous JSON ingestion using Auto Loader
- Combining batch and streaming data into the same analytical pipeline
- Declarative Lakeflow pipeline
- Data quality expectations
- Scheduled Databricks production jobs
- Task dependencies and monitoring
- Additional customer and product analytics

## Repository Structure

A possible repository structure is:

```text
ShopStream/
│
├── README.md
│
├── dashboard.png
│
├── notebooks/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── pipelines/
│   └── ...
│
└── jobs/
    └── ...
```

> Note: The exact repository structure can be adapted to match the Databricks workspace and exported notebooks.

## Key Learning Outcomes

Through this project, I am practicing how to build a complete data pipeline rather than working with isolated technologies.

The project connects:

```text
Python / SQL
     ↓
PySpark
     ↓
Delta Lake
     ↓
Databricks
     ↓
Medallion Architecture
     ↓
Batch + Streaming
     ↓
Gold Analytics
     ↓
Dashboard
     ↓
Production Data Pipeline
```

## Author

**Youssef Alaraby**

Data Engineering project built as a hands-on Databricks lakehouse implementation.

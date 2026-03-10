# End-to-End Lakehouse & BI Solution for Olist E-commerce Dataset

**Tools:** Microsoft Fabric, PySpark, Delta Lake, Data Pipelines, Power BI  
**Architecture:** Medallion Architecture (Bronze → Silver → Gold)

This project demonstrates the implementation of a modern **Lakehouse analytics architecture** using **Microsoft Fabric**. The objective was to transform raw e-commerce data into a structured analytics model capable of supporting business intelligence reporting.

The system ingests raw datasets, cleans and standardizes them using distributed processing, and produces curated analytical tables that power dashboards and business insights.

---

# Architecture Overview

The project follows the **Medallion Architecture**, which separates data processing into three logical layers.


<img width="1536" height="1024" alt="Architecture Diagram" src="https://github.com/user-attachments/assets/fc6eebbe-fc99-490b-87d0-baa153ac7a99" />



---

# Dataset

The project uses the **Olist Brazilian E-commerce dataset**, which contains multiple interconnected datasets representing real-world marketplace operations.

Main datasets include:

- orders
- order_items
- order_payments
- order_reviews
- customers
- sellers
- products
- geolocation
- product_category_translation

These datasets provide insights into **sales transactions, logistics, product categories, customer behavior, and seller performance**.

---

# Why Lakehouse Instead of a Data Warehouse

This project uses a **Lakehouse architecture rather than a traditional data warehouse**.

Advantages include:

**Flexibility**

Supports structured and semi-structured datasets.

**Scalability**

Designed for distributed data processing and large-scale analytics.

**Unified Analytics**

Combines data engineering and business intelligence within the same platform.

**Open Storage Format**

Data is stored using open formats such as Delta rather than proprietary warehouse storage.

---

# Bronze Layer – Raw Data Ingestion

The Bronze layer serves as the **initial landing zone for raw data**.

## Data Source

The datasets were uploaded to GitHub and ingested using a **Microsoft Fabric Data Pipeline**.

The pipeline extracts the CSV datasets from GitHub and loads them directly into the **Bronze Lakehouse**.

<img width="1144" height="746" alt="Pipeline" src="https://github.com/user-attachments/assets/881bb980-3b6d-4363-baf2-73bc1d58c524" />


## Storage Format

Data in the Bronze layer remains in **CSV format**.

### Why CSV in Bronze?

Keeping the original format ensures:

- The raw data remains unchanged
- The ingestion pipeline stays simple
- Data can be reprocessed if transformation logic changes

At this stage **no transformations are applied**.

---

# Silver Layer – Data Cleaning & Standardization

The Silver layer transforms raw datasets into **clean, standardized tables suitable for analytics**.

All transformations were implemented using **PySpark notebooks in Microsoft Fabric**.

## Why PySpark Notebooks Instead of Dataflows?

PySpark notebooks provide several advantages.

**Scalability**

Distributed processing makes it suitable for large datasets.

**Flexibility**

Complex joins, transformations, and custom logic are easier to implement.

**Reproducibility**

Transformation logic can be version controlled and reused.

**Advanced Processing**

Python enables more powerful data manipulation than low-code tools.

For these reasons PySpark notebooks were used for the **data engineering transformation layer**.

---

## Transformations Performed

### Text Standardization

- Converted city names to lowercase  
- Trimmed whitespace  
- Removed accented characters  

### Data Type Conversion

Converted string columns into proper types:

- timestamps
- numeric values
- integer review scores

### Data Cleaning

- Removed duplicate records
- Standardized product categories

---

## Storage Format in Silver

Cleaned datasets were stored in **Delta format**.

### Why Delta?

Delta Lake provides:

- ACID transactions
- Schema enforcement
- Version control (time travel)
- Faster query performance

This makes Delta ideal for curated intermediate datasets.

---

# Gold Layer – Analytical Data Model

The Gold layer contains **business-ready tables designed for analytics and reporting**.

These tables were generated from the Silver layer using PySpark notebooks.

The goal of this layer is to reduce complexity in BI tools and enable efficient queries.

---

## Fact Table

### fact_orders

A consolidated fact table combining information from multiple datasets:

- orders
- order_items
- payments
- reviews
- products

Metrics include:

- total item value
- freight value
- payment totals
- review score
- delivery time

This table represents the **core transactional dataset for analytics**.

---

## Dimension Tables

The model includes four dimension tables:

**dim_customers**  
Customer attributes and geographic information.

**dim_products**  
Product attributes and translated product categories.

**dim_sellers**  
Seller identification and location.

**dim_date**  
Calendar table used for time-based analytics.

---

## Aggregated Tables

Additional aggregated tables were created to support analytics.

**agg_product_monthly**

Monthly product category trends.

**agg_customer_ltv**

Customer lifetime value metrics.

**agg_seller_performance**

Seller performance and delivery efficiency metrics.

These aggregated tables reduce query complexity and improve dashboard performance.

---

# Data Model

The Gold layer forms a **snowflake schema**.

```
               dim_products
                     │
dim_customers ── fact_orders ── dim_sellers 
                     │
                  dim_date
```

Aggregated tables connect to dimensions for analytical queries.

<img width="1196" height="661" alt="Data Model" src="https://github.com/user-attachments/assets/fcc3338d-e388-4d48-8e6e-bef3e87994b6" />

---

# Semantic Model Layer

After building the Gold layer, a **Power BI Semantic Model** was created before building dashboards.

Instead of connecting directly to the Gold tables using DirectQuery or Direct Lake, the model was imported using **Import Mode**.

---

## Why Use Import Mode?

The decision was based on two characteristics of this project:

- The dataset size is relatively **small**
- There is **no requirement for near real-time updates**

Advantages of Import Mode include:

**Fast Dashboards**

Data is stored in memory, resulting in faster visual interactions.

**Simpler Development**

Relationships, calculations, and measures are easier to manage.

**Lower Compute Load**

Queries do not constantly hit the lakehouse storage.

**Better Debugging**

Data modeling and troubleshooting become easier during development.

---

## Trade-offs: Import vs Direct Query

| Feature | Import Mode | Direct Query / Direct Lake |
|--------|-------------|-----------------------------|
| Query Speed | Very Fast | Slower |
| Compute Usage | Low | Higher |
| Data Freshness | Requires refresh | Near Real-Time |
| Development Complexity | Easier | More complex |
| Dataset Size | Best for small / medium | Best for large datasets |

For this project **Import Mode provided the best balance of performance and simplicity**.

If the dataset were significantly larger or required real-time analytics, **DirectQuery or Direct Lake would be more appropriate**.

---

# Business Insights Enabled

The analytical model supports dashboards providing insights into:

**Sales Performance**

Revenue trends and product category performance.

**Customer Analytics**

Customer lifetime value and repeat purchasing behavior.

**Seller Performance**

Revenue contribution and delivery efficiency.

**Logistics Insights**

On-time delivery rates and average delivery duration.

---

# Tech Stack

| Component | Technology |
|-----------|------------|
| Data Platform | Microsoft Fabric |
| Data Processing | PySpark |
| Storage Format | Delta Lake |
| Data Orchestration | Fabric Data Pipeline |
| Visualization | Power BI |
| Architecture | Medallion Architecture |

---

# Key Learnings

This project demonstrates experience in:

- Designing a modern **Lakehouse architecture**
- Building **data ingestion pipelines**
- Performing **distributed data transformations**
- Designing **analytical data models**
- Delivering insights using **business intelligence dashboards**

---

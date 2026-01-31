# Olist E-Commerce ELT Pipeline

A production-ready ELT (Extract, Load, Transform) pipeline that orchestrates the ingestion and transformation of Brazilian e-commerce data. This project leverages Docker for environment isolation, PostgreSQL as the data warehouse, and dbt for dimensional modeling and data quality assurance.

## Architecture and Tech Stack
* **Orchestration:** Docker and Docker Compose
* **Storage:** PostgreSQL (OLAP)
* **Transformation:** dbt (Data Build Tool)
* **Scripting:** Python (ETL) and Bash (Workflow Automation)

---

## Data Modeling and Lineage
The transformation layer is built on a modular design, moving from raw source tables to optimized analytical marts. The Directed Acyclic Graph (DAG) illustrates the dependencies between dimensional models and business views.

### Lineage Graph
![SQL_LOGIC](./lineage_diagram.png)
* **Source Layer:** Ingested raw data including dim_customers, fact_orders, dim_products, and fact_order_items.
* **Transformation Layer:** Modular SQL logic for complex metrics such as Customer Lifetime Value (LTV) and Monthly Revenue Growth.
* **Mart Layer:** Business-ready tables like mart_category_profit, designed for direct consumption by BI tools.

---

## Data Quality and Reliability
To ensure data integrity, I implemented an automated testing suite within the dbt lifecycle:

* **Custom Business Logic Tests:** Created `assert_positive_revenue` to catch cases where revenue might be reported as zero or negative before it reaches final reporting.
* **Schema Validation:** Enforced `unique` and `not_null` constraints on primary keys across all fact and dimension tables.

---

## Sample Compiled SQL: mart_category_profit
This model demonstrates the logic used to calculate profitability at the category level, including shipping margins and volume filtering.

![SQL_LOGIC](./mart_category-profit.png)
```sql
SELECT
    p.product_category_name,
    COUNT(i.order_id) as total_sales_count,
    SUM(i.price) as total_revenue,
    SUM(i.freight_value) as total_shipping_costs,
    ROUND((SUM(i.price) / (SUM(i.price) + SUM(i.freight_value)))::numeric, 3) as margin_ratio
FROM dim_products p
JOIN fact_order_items i ON p.product_id = i.product_id
GROUP BY 1
HAVING COUNT(i.order_id) > 100
ORDER BY margin_ratio DESC
```
---
## Cloning
Follow these instructions to replicate the environment and run the data pipeline locally.

### Prerequisites
Docker and Docker Compose installed.

### Git installed.

A PostgreSQL client (optional, for manual verification).

### Installation and Setup
Clone the repository:

```
git clone https://github.com/ananyagullapally/olist-docker-pipeline.git
cd olist-docker-pipeline
```
Launch the stack:

```
docker-compose up -d
```
Execute the Pipeline:

```
bash run_pipeline.sh
```

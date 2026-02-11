# Containerized ELT Pipeline

A production-ready ELT (Extract, Load, Transform) pipeline that orchestrates the ingestion and transformation of [Brazilian e-commerce data](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). This project leverages Docker for environment isolation, PostgreSQL as the data warehouse, and dbt for dimensional modeling and data quality assurance.

## Architecture and Tech Stack
![SQL_LOGIC](./assets/architecture-diagram-olist.png)
* **Orchestration:** Docker and Docker Compose
* **Storage:** PostgreSQL (OLAP)
* **Transformation:** dbt (Data Build Tool)
* **Scripting:** Python (ELT) and Bash (Workflow Automation)
---
## Project Structure
```
.
├── assets/               # Screenshots of SQL logic, Lineage, and Dashboards
├── data/                 # Raw Olist CSV files (Local only, ignored by Git)
├── scripts/              # SQL transformation scripts
├── .gitignore            # Prevents .env and data/ from being uploaded
├── docker-compose.yml    # Defines Postgres and App containers
├── main.py               # Python logic for Loading data
├── README.md             # Project documentation
└── run_pipeline.sh       # Bash script to orchestrate the entire flow
```
---

## Data Modeling and Lineage
I implemented a **Star Schema** approach (or whatever your SQL code builds) to optimize for analytical queries:
- **Fact Table:** `fact_orders` (containing order values and timestamps).
- **Dimension Tables:** `dim_customers`, `dim_products`, and `dim_sellers`.

### Lineage Graph
![SQL_LOGIC](./assets/lineage_diagram.png)
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

![SQL_LOGIC](./assets/mart_category_profit.png)
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
## Key Challenges & Lessons Learned

### 1. Data Integrity during Transformation
* **Challenge:** I discovered that performing joins in the **Transform (T)** stage could lead to duplicate records or data fan-out if primary keys weren't unique across the Olist source files.
* **Solution:** I implemented strict primary key verification and deduplication logic within the SQL transformation scripts before final table materialization.
* **Lesson:** Always validate the "grain" of your data before joining tables in a data warehouse environment.

### 2. Container Orchestration & Race Conditions
* **Challenge:** The Python ingestion script would occasionally fail because it attempted to connect to PostgreSQL before the Docker container was fully healthy and ready to accept connections.
* **Solution:** I implemented a **wait-for-it** style health check in `run_pipeline.sh`. This ensures the database port is open and the service is responsive before triggering the Python loader.
* **Lesson:** Infrastructure readiness is just as critical as code logic in containerized environments.

### 3. Git Workflow & Asset Management
* **Challenge:** Managing large image assets for documentation and sensitive environment variables in a remote VM threatened to clutter the repository and compromise security.
* **Solution:**
    * Leveraged a dedicated `assets/` folder to maintain a clean root directory.
    * Strictly enforced `.gitignore` policies to ensure `.env` and raw `/data` files never reached the public repository.
* **Lesson:** Professional repository management requires a clear separation between code, configuration, and documentation.
---
## Key Business Findings

![SQL_LOGIC](./assets/monthly_revenue_trend.png)
![SQL_LOGIC](./assets/revenue_by_city_report.png)
![SQL_LOGIC](./assests/mart_category_profit.png)
### 1. Revenue is highly concentrated across product categories
**Observation**  
A small subset of product categories contributes a disproportionately large share of total revenue, while many categories generate minimal sales.

**Deduction**  
The marketplace follows a Pareto-style distribution, where growth is driven by a few dominant categories rather than broad-based participation.

**Business Implication**  
Strategic focus on high-performing categories (pricing, promotions, seller quality) would deliver greater ROI than uniform category expansion.

---

### 2. High order volume does not imply high revenue
**Observation**  
Some categories have high order counts but low total revenue, while others generate high revenue with fewer orders.

**Deduction**  
There is a clear split between high-frequency, low-ticket items and low-frequency, high-value items.

**Business Implication**  
Order volume alone is an incomplete performance metric; Average Order Value (AOV) must be evaluated alongside volume.

---

### 3. Delivery delays are systemic, not random
**Observation**  
A significant portion of orders are delivered after their estimated delivery dates, with delays clustering by seller and region.

**Deduction**  
Late deliveries are driven by structural logistics issues rather than isolated incidents.

**Business Implication**  
Seller-level SLA monitoring and regional logistics optimization would improve customer experience more effectively than generic delivery promises.

---

### 4. Seller performance is highly uneven
**Observation**  
A small group of sellers accounts for a large share of fulfilled orders, while many sellers show minimal activity.

**Deduction**  
The marketplace is operationally dependent on a limited core of high-performing sellers.

**Business Implication**  
Protecting and incentivizing top sellers is critical, while underperforming sellers represent both noise and operational risk.

---

### 5. Customer repeat behavior is limited
**Observation**  
Most customers appear only once or very few times in the dataset.

**Deduction**  
Customer acquisition is strong, but retention and repeat purchasing behavior are weak.

**Business Implication**  
Retention-focused strategies (loyalty programs, faster delivery, personalized offers) could significantly increase customer lifetime value.

---

### 6. Geography impacts both demand and fulfillment
**Observation**  
Order volume, revenue, and delivery performance vary noticeably by region.

**Deduction**  
Geographic factors influence both purchasing behavior and logistics efficiency.

**Business Implication**  
Region-specific strategies such as localized sellers or regional fulfillment hubs would outperform a single nationwide approach.

---

### 7. Installment payments play a key role in conversions
**Observation**  
A large portion of orders use installment-based payments rather than single upfront payments.

**Deduction**  
Customers are sensitive to upfront cost, even for mid-range purchases.

**Business Implication**  
Flexible payment options act as a conversion driver, especially for higher-value categories.

---

## How the ELT Pipeline Enables These Insights

The ELT pipeline is not just infrastructure — it is the reason these insights are reliable.

### Metric correctness through controlled grain
- Orders, items, payments, customers, and sellers are modeled at clearly defined grains
- Prevents revenue inflation caused by fan-out joins

**Impact:**  
Revenue concentration and AOV insights reflect real business behavior, not SQL artifacts.

---

### Clean aggregations and normalization
- Separates order counts from monetary values
- Normalizes payment and installment data

**Impact:**  
Enables accurate comparison across categories, sellers, and regions.

---

### Standardized timestamps and derived metrics
- Normalizes delivery and order timestamps
- Derives delivery delay metrics

**Impact:**  
Transforms delivery performance from anecdotal issues into measurable operational signals.

---

### Dimensional modeling for sellers, customers, and geography
- Explicit seller, customer, and location dimensions
- Enforced relationships across fact tables

**Impact:**  
Makes seller concentration, customer retention, and regional disparities analytically defensible.

---

## Key Takeaway

> The ELT pipeline does not create insights — it removes ambiguity.

By enforcing correct data grain, consistent transformations, and reproducible modeling, this pipeline converts raw marketplace data into **trustworthy business intelligence** that can support real operational and strategic decisions.

---

## Future Extensions
- BI dashboards built on analytics-ready tables
- Seller performance scoring models
- Customer lifetime value and churn prediction
- Regional logistics optimization analysis

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

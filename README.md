# Olist E-Commerce ELT Pipeline 

A production-ready ELT (Extract, Load, Transform) pipeline designed to process Brazilian e-commerce data. This project demonstrates a modern data engineering workflow using **Python**, **PostgreSQL**, **dbt**, and **Docker**.



## 🛠️ Architecture & Tools
- **Orchestration:** Python (Pandas) for automated data extraction and loading.
- **Data Warehouse:** PostgreSQL (Relational Database) for storage.
- **Transformations:** dbt (data build tool) for SQL-based modeling and KPI generation.
- **Containerization:** Docker for a fully reproducible environment.
- **Analytics:** Automated generation of Profit Margin, Monthly Revenue, and Customer LTV charts.

## 🔄 The ELT Workflow
Unlike traditional ETL, this project follows an **ELT** approach:
1. **Extract & Load:** Python scripts read raw Olist datasets and load them directly into PostgreSQL "as-is" to preserve data lineage.
2. **Transform:** dbt models execute SQL inside the warehouse to clean data, join tables, and build analytical "Marts."
3. **Visualize:** A final Python layer queries the dbt-transformed views to generate business intelligence visualizations.

## How to Run (Docker)
This project is fully containerized. To run the entire pipeline with a single command:

### Prerequisites
- Docker installed
- PostgreSQL running on the host machine (or as a sibling container)

### Execution
1. Clone the repository
\`\`\`bash
git clone [https://github.com/ananyagullapally/olist-docker-pipeline.git](https://github.com/ananyagullapally/olist-docker-pipeline.git)
cd olist-docker-pipeline
\`\`\`

2. Build the Docker image
\`\`\`bash
docker build -t olist-pipeline .
\`\`\`

3. Run the pipeline
\`\`\`bash
docker run --network="host" -v $(pwd):/app olist-pipeline
\`\`\`

import os
import sys
import logging
import subprocess
import pandas as pd
from sqlalchemy import create_engine, text
from src.extract import extract_csv
from src.transform import generate_revenue_report

# 1. Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True,
    handlers=[
        logging.StreamHandler(sys.stdout)  # This sends logs to your terminal
    ]
)

# 2. Define the dbt trigger function (Top Level)
def run_dbt():
    print("Running dbt transformations...")
    original_dir = os.getcwd()
    try:
        os.chdir('/app/olist_analytics')
        dbt_executable = 'dbt'
        result = subprocess.run([dbt_executable, "build","--profiles-dir","."], capture_output=False, text=True)
        if result.returncode == 0:
            print("dbt transformations successful!")
        else:
            raise Exception("dbt transformation failed")
    finally:
        os.chdir(original_dir)

# 3. Define the main pipeline function (Top Level)
def run_pipeline():
    print("THE PIPELINE IS NOW RUNNING...")
    db_host = os.getenv('DB_HOST', 'localhost')
    db_url = f'postgresql://ananya:1234@{db_host}:5432/ecommerce_dw'
    engine = create_engine(db_url)
    
    datasets = {
        "olist_orders_dataset.csv": "fact_orders",
        "olist_customers_dataset.csv": "dim_customers",
        "olist_products_dataset.csv": "dim_products",
        "olist_order_items_dataset.csv": "fact_order_items"
    }

    try:
        # Step 1: Extract & Load
        for file_name, table_name in datasets.items():
            logging.info(f"Extracting {file_name}...")
            df = extract_csv(file_name)
            print(f"Loading {table_name} to Postgres...")
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE;"))
                conn.commit()
            df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        # Step 2: Transform (dbt)
        run_dbt()

        # Step 3: Report
        print("Step 3: Generating Final Reports...")
        generate_revenue_report()
        print("Full Pipeline Complete!")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")

if __name__ == "__main__":
    run_pipeline()

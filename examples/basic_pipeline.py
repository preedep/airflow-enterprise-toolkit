"""
Basic Control-M to Airflow Migration Example

This example demonstrates a simple data pipeline migrated from Control-M to Airflow
using the Airflow Enterprise Toolkit base classes.

Original Control-M workflow:
1. DataExtraction - Extract data from source system
2. DataTransformation - Transform extracted data (depends on DataExtraction)
3. DataLoading - Load transformed data to warehouse (depends on DataTransformation)
"""

from datetime import datetime, timedelta
from airflow_enterprise_toolkit.dag_base import create_control_m_dag

# Create a Control-M compatible DAG
dag = create_control_m_dag(
    dag_id="control_m_basic_pipeline",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    order_date_strategy="previous_business_day",
    tags=["control-m", "data-pipeline", "migration"],
    description="Basic data pipeline migrated from Control-M"
)

# Create tasks using Control-M style
extract_task = dag.create_control_m_task(
    task_id="data_extraction",
    command="python3 /opt/scripts/extract_data.py --date={{ order_date }} --source=production",
    job_type="command",
    order_date_param=True,
    retries=2,
    retry_delay=timedelta(minutes=10)
)

transform_task = dag.create_control_m_task(
    task_id="data_transformation", 
    command="python3 /opt/scripts/transform_data.py --input={{ order_date }}_raw.csv --output={{ order_date }}_transformed.csv",
    job_type="command",
    order_date_param=True,
    retries=1,
    retry_delay=timedelta(minutes=5)
)

load_task = dag.create_control_m_task(
    task_id="data_loading",
    command="python3 /opt/scripts/load_data.py --file={{ order_date }}_transformed.csv --target=warehouse",
    job_type="command", 
    order_date_param=True,
    retries=3,
    retry_delay=timedelta(minutes=15)
)

# Set up dependencies (Control-M style)
extract_task >> transform_task >> load_task

# Add documentation
extract_task.doc_md = """
### Data Extraction Task

This task extracts data from the source production system.
- **Source**: Production database
- **Format**: CSV
- **Order Date**: Uses previous business day
- **Retries**: 2 with 10-minute intervals
"""

transform_task.doc_md = """
### Data Transformation Task

This task transforms the extracted data according to business rules.
- **Input**: {{ order_date }}_raw.csv
- **Output**: {{ order_date }}_transformed.csv  
- **Dependencies**: Data Extraction
- **Retries**: 1 with 5-minute intervals
"""

load_task.doc_md = """
### Data Loading Task

This task loads the transformed data into the data warehouse.
- **Source**: {{ order_date }}_transformed.csv
- **Target**: Data warehouse
- **Dependencies**: Data Transformation
- **Retries**: 3 with 15-minute intervals
"""

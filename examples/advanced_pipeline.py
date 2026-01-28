"""
Advanced Control-M to Airflow Migration Example

This example demonstrates a complex data pipeline with advanced Control-M features:
- Multiple job types (Command, Python, Database)
- Hold conditions (file holds, time holds)
- Complex dependencies
- Order date handling
- Environment variables
"""

from datetime import datetime, timedelta
from airflow_enterprise_toolkit.dag_base import create_control_m_dag
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Create a Control-M compatible DAG with advanced configuration
dag = create_control_m_dag(
    dag_id="control_m_advanced_pipeline",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    order_date_strategy="offset:-1",  # Previous day
    tags=["control-m", "advanced", "data-pipeline"],
    description="Advanced data pipeline with Control-M features",
    default_args={
        'owner': 'data-engineering',
        'email': ['data-team@company.com'],
        'email_on_failure': True,
        'email_on_retry': False,
    }
)

# Task 1: Wait for input file (File Hold)
wait_for_input_file = dag.create_file_hold_operator(
    task_id="wait_for_input_file",
    file_path="/data/input/{{ order_date }}_source.csv",
    poke_interval=300,  # Check every 5 minutes
    timeout=3600,      # Timeout after 1 hour
)

# Task 2: Data validation using Python
def validate_input_data(**context):
    """Validate input data file format and content."""
    import os
    import pandas as pd
    
    order_date = context['ds']
    file_path = f"/data/input/{order_date}_source.csv"
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file not found: {file_path}")
    
    # Read and validate CSV
    try:
        df = pd.read_csv(file_path)
        if df.empty:
            raise ValueError("Input file is empty")
        
        # Check required columns
        required_columns = ['id', 'name', 'value', 'timestamp']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        print(f"Validation successful: {len(df)} records found")
        return True
        
    except Exception as e:
        raise ValueError(f"Data validation failed: {str(e)}")

validate_data = PythonOperator(
    task_id="validate_data",
    python_callable=validate_input_data,
    dag=dag,
)

# Task 3: Database cleanup (SQL operation)
cleanup_staging = PostgresOperator(
    task_id="cleanup_staging",
    postgres_conn_id="postgres_warehouse",
    sql="""
    DELETE FROM staging.processed_data 
    WHERE process_date = '{{ order_date }}';
    """,
    dag=dag,
)

# Task 4: Data processing with environment variables
process_data = dag.create_control_m_task(
    task_id="process_data",
    command="""
    python3 /opt/scripts/process_data.py \
        --input=/data/input/{{ order_date }}_source.csv \
        --output=/data/processed/{{ order_date }}_processed.csv \
        --log-level=INFO
    """,
    job_type="command",
    order_date_param=True,
    env={
        'PYTHONPATH': '/opt/scripts',
        'DATA_QUALITY_CHECK': 'true',
        'MAX_RECORDS': '1000000',
    },
    retries=2,
    retry_delay=timedelta(minutes=10),
)

# Task 5: Wait for business hours (Time Hold)
wait_for_business_hours = dag.create_time_hold_operator(
    task_id="wait_for_business_hours",
    target_time="09:00",
)

# Task 6: Load to warehouse
load_to_warehouse = PostgresOperator(
    task_id="load_to_warehouse",
    postgres_conn_id="postgres_warehouse",
    sql="""
    INSERT INTO warehouse.fact_data (
        id, name, value, timestamp, process_date, created_at
    )
    SELECT 
        id, name, value, timestamp, '{{ order_date }}', NOW()
    FROM staging.processed_data
    WHERE process_date = '{{ order_date }}';
    
    -- Update statistics
    ANALYZE warehouse.fact_data;
    """,
    dag=dag,
)

# Task 7: Generate report
generate_report = dag.create_control_m_task(
    task_id="generate_report",
    command="""
    python3 /opt/scripts/generate_report.py \
        --date={{ order_date }} \
        --output=/reports/{{ order_date }}_summary.pdf \
        --format=pdf
    """,
    job_type="command",
    order_date_param=True,
)

# Task 8: Send notification
def send_notification(**context):
    """Send success notification."""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    
    order_date = context['ds']
    
    msg = MIMEMultipart()
    msg['From'] = 'airflow@company.com'
    msg['To'] = 'data-team@company.com'
    msg['Subject'] = f'Data Pipeline Completed - {order_date}'
    
    body = f"""
    The advanced data pipeline has completed successfully for {order_date}.
    
    Tasks completed:
    - Data validation: ✓
    - Data processing: ✓
    - Warehouse loading: ✓
    - Report generation: ✓
    
    Reports are available at: /reports/{order_date}_summary.pdf
    """
    
    msg.attach(MIMEText(body, 'plain'))
    
    # Send email (configuration should be in Airflow connections)
    # server = smtplib.SMTP('smtp.company.com')
    # server.send_message(msg)
    # server.quit()
    
    print(f"Notification sent for {order_date}")

send_email = PythonOperator(
    task_id="send_notification",
    python_callable=send_notification,
    dag=dag,
    trigger_rule='all_success',  # Only send if all upstream tasks succeed
)

# Set up complex dependencies
# Linear flow with parallel branches
wait_for_input_file >> validate_data >> cleanup_staging >> process_data

# After processing, wait for business hours then load
process_data >> wait_for_business_hours >> load_to_warehouse

# Generate report and send notification in parallel after loading
load_to_warehouse >> [generate_report, send_email]

# Add task documentation
wait_for_input_file.doc_md = """
### Wait for Input File

Waits for the input CSV file to be available.
- **File Pattern**: `/data/input/{{ order_date }}_source.csv`
- **Check Interval**: 5 minutes
- **Timeout**: 1 hour
- **Type**: File Hold (Control-M feature)
"""

validate_data.doc_md = """
### Data Validation

Validates the input data file format and content.
- **Checks**: File existence, format, required columns
- **Error Handling**: Raises exception on validation failure
- **Type**: Python custom operator
"""

cleanup_staging.doc_md = """
### Cleanup Staging Area

Removes old data from the staging area.
- **Target**: `staging.processed_data` table
- **Scope**: Only removes data for current order date
- **Type**: PostgreSQL operator
"""

process_data.doc_md = """
### Data Processing

Processes the input data and generates transformed output.
- **Script**: `/opt/scripts/process_data.py`
- **Environment**: Custom environment variables
- **Retries**: 2 with 10-minute intervals
- **Type**: Control-M command task
"""

wait_for_business_hours.doc_md = """
### Wait for Business Hours

Waits until 9:00 AM before proceeding.
- **Target Time**: 09:00
- **Purpose**: Ensure warehouse loading during business hours
- **Type**: Time Hold (Control-M feature)
"""

load_to_warehouse.doc_md = """
### Load to Warehouse

Loads processed data into the data warehouse.
- **Target**: `warehouse.fact_data` table
- **Operations**: INSERT + ANALYZE
- **Type**: PostgreSQL operator
"""

generate_report.doc_md = """
### Generate Report

Generates daily summary report in PDF format.
- **Script**: `/opt/scripts/generate_report.py`
- **Output**: `/reports/{{ order_date }}_summary.pdf`
- **Type**: Control-M command task
"""

send_email.doc_md = """
### Send Notification

Sends completion notification to the data team.
- **Recipients**: data-team@company.com
- **Trigger**: Only on successful completion of all tasks
- **Type**: Python notification operator
"""

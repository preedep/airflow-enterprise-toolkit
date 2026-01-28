"""
Control-M migration DAG containing 5 jobs

This DAG was automatically generated from Control-M jobs using the Airflow Enterprise Toolkit.
Original jobs: 5
"""
from airflow import DAG
from airflow.macros import ds_add
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': 2026, 01, 28,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'control_m_group_dataextraction',
    default_args=default_args,
    description=Control-M migration DAG containing 5 jobs,
    schedule_interval='MON,TUE,WED,THU,FRI',
    catchup=False,
    tags=["control-m-migration"],
    max_active_runs=1,
    is_paused=False,
)

# Task definitions
# dataextraction - BashOperator
# Order date handling: Current
dataextraction = BashOperator(
    task_id='dataextraction',
    dag=dag,
    env={"LOG_LEVEL": "INFO", "SOURCE_SYSTEM": "production"},
    bash_command="python3 /opt/scripts/extract_data.py --date={{ORDER_DATE}}",
    execution_timeout=1800,
)
# Pass order date as parameter
dataextraction.op_kwargs['order_date'] = {{ ds }}

# datatransformation - BashOperator
# Order date handling: Current
datatransformation = BashOperator(
    task_id='datatransformation',
    dag=dag,
    execution_timeout=2700,
    bash_command="python3 /opt/scripts/transform_data.py --input={{ORDER_DATE}}_raw.csv",
    env={"BATCH_SIZE": "10000", "TRANSFORMATION_RULES": "/config/rules.json"},
)
# Pass order date as parameter
datatransformation.op_kwargs['order_date'] = {{ ds }}

# dataloading - BashOperator
# Order date handling: Current
dataloading = BashOperator(
    task_id='dataloading',
    dag=dag,
    env={"TARGET_TABLE": "fact_data", "WAREHOUSE_CONN": "warehouse_prod"},
    bash_command="python3 /opt/scripts/load_data.py --file={{ORDER_DATE}}_transformed.csv",
    execution_timeout=3600,
)
# Pass order date as parameter
dataloading.op_kwargs['order_date'] = {{ ds }}

# healthcheck - BashOperator
# Order date handling: Current
healthcheck = BashOperator(
    task_id='healthcheck',
    dag=dag,
    bash_command="python3 /opt/scripts/health_check.py",
    execution_timeout=3600,
)
# Pass order date as parameter
healthcheck.op_kwargs['order_date'] = {{ ds }}

# cleanuptempfiles - BashOperator
# Order date handling: Current
cleanuptempfiles = BashOperator(
    task_id='cleanuptempfiles',
    dag=dag,
    bash_command="find /tmp -name "*.tmp" -mtime +1 -delete",
    execution_timeout=3600,
)
# Pass order date as parameter
cleanuptempfiles.op_kwargs['order_date'] = {{ ds }}


# Task dependencies
    datatransformation.set_upstream([dataextraction])
    dataloading.set_upstream([datatransformation])

# Order date handling
# Order date configuration for dataextraction
# Strategy: Current
# Template: {{ ds }}
# Order date configuration for datatransformation
# Strategy: Current
# Template: {{ ds }}
# Order date configuration for dataloading
# Strategy: Current
# Template: {{ ds }}
# Order date configuration for healthcheck
# Strategy: Current
# Template: {{ ds }}
# Order date configuration for cleanuptempfiles
# Strategy: Current
# Template: {{ ds }}

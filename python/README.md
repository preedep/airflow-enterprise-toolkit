# Airflow Enterprise Toolkit - Python Module

This directory contains Python base classes for creating Airflow DAGs from Control-M jobs.

## Installation

### Prerequisites

This module requires Apache Airflow 3.0.0 or later. We recommend using Python 3.9-3.12 for best compatibility.

### Setup

1. Create a virtual environment:
```bash
python3.9 -m venv venv
source venv/bin/activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

### Usage

```python
from dag_base import create_control_m_dag, convert_control_m_dependencies

# Create a Control-M compatible DAG
dag = create_control_m_dag(
    dag_id="control_m_migration_example",
    schedule="@daily",
    order_date_strategy="previous_business_day"
)

# Create tasks
task1 = dag.create_control_m_task(
    task_id="extract_data",
    command="python3 extract.py --date={{ order_date }}",
    order_date_param=True
)

task2 = dag.create_control_m_task(
    task_id="transform_data", 
    command="python3 transform.py"
)

# Set up dependencies
task1 >> task2
```

## Features

- **Order Date Handling**: Supports Control-M order date strategies
- **Job Hold Conditions**: File and time-based holds
- **Task Groups**: Organize related tasks
- **Dependency Management**: Convert Control-M dependencies to Airflow

## Troubleshooting

If you encounter import errors, ensure:
1. You're using Python 3.9-3.12
2. Apache Airflow 3.x is properly installed
3. All dependencies from requirements.txt are installed
4. Run `python test_imports.py` to verify your installation

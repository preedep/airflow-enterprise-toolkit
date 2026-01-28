"""
Base classes for Airflow DAGs generated from Control-M jobs.

This module provides base classes and utilities for creating Airflow DAGs
that properly handle Control-M specific features like order dates and job holds.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from airflow import DAG
from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk import TaskGroup
import logging

logger = logging.getLogger(__name__)


class ControlMOrderDateMixin:
    """
    Mixin class to handle Control-M order date functionality.
    
    Control-M uses order dates to determine the business date for job execution.
    This mixin provides equivalent functionality in Airflow.
    """
    
    @staticmethod
    def get_order_date(execution_date: datetime, strategy: str = "current") -> str:
        """
        Get order date based on Control-M strategy.
        
        Args:
            execution_date: Airflow execution date
            strategy: Order date strategy ("current", "previous_business_day", "offset:N")
            
        Returns:
            Order date as string in YYYY-MM-DD format
        """
        from airflow.macros import ds_add
        
        if strategy == "current":
            return execution_date.strftime('%Y-%m-%d')
        elif strategy == "previous_business_day":
            # Simple previous business day calculation
            # In production, you might want to use a holiday calendar
            prev_day = ds_add(execution_date.strftime('%Y-%m-%d'), -1)
            # Skip weekends
            if datetime.strptime(prev_day, '%Y-%m-%d').weekday() >= 5:  # Saturday or Sunday
                return ds_add(prev_day, -2)  # Go back to Friday
            return prev_day
        elif strategy.startswith("offset:"):
            try:
                offset = int(strategy.split(":")[1])
                return ds_add(execution_date.strftime('%Y-%m-%d'), offset)
            except (ValueError, IndexError):
                logger.warning(f"Invalid offset strategy: {strategy}, using current")
                return execution_date.strftime('%Y-%m-%d')
        else:
            logger.warning(f"Unknown order date strategy: {strategy}, using current")
            return execution_date.strftime('%Y-%m-%d')


class ControlMJobHoldMixin:
    """
    Mixin class to handle Control-M job hold conditions.
    
    Control-M supports various hold conditions like file holds, time holds, etc.
    This mixin provides equivalent functionality in Airflow.
    """
    
    @staticmethod
    def create_file_hold_operator(
        task_id: str,
        file_path: str,
        poke_interval: int = 60,
        timeout: int = 3600,
        **kwargs
    ) -> BaseOperator:
        """
        Create a file hold operator that waits for a file to exist.
        
        Args:
            task_id: Task identifier
            file_path: Path to file to wait for
            poke_interval: Check interval in seconds
            timeout: Timeout in seconds
            **kwargs: Additional operator arguments
            
        Returns:
            File sensor operator
        """
        try:
            from airflow.sensors.filesystem import FileSensor
            
            return FileSensor(
                task_id=task_id,
                filepath=file_path,
                poke_interval=poke_interval,
                timeout=timeout,
                mode='reschedule',
                **kwargs
            )
        except ImportError:
            # Fallback for older Airflow versions
            from airflow.sensors import FileSensor
            
            return FileSensor(
                task_id=task_id,
                filepath=file_path,
                poke_interval=poke_interval,
                timeout=timeout,
                mode='reschedule',
                **kwargs
            )
    
    @staticmethod
    def create_time_hold_operator(
        task_id: str,
        target_time: str,
        **kwargs
    ) -> BaseOperator:
        """
        Create a time hold operator that waits until a specific time.
        
        Args:
            task_id: Task identifier
            target_time: Target time in HH:MM format
            **kwargs: Additional operator arguments
            
        Returns:
            Time sensor operator
        """
        try:
            from airflow.sensors.time_sensor import TimeSensor
            
            return TimeSensor(
                task_id=task_id,
                target_time=target_time,
                **kwargs
            )
        except ImportError:
            # Fallback implementation using BashOperator
            from airflow.operators.bash import BashOperator
            
            bash_command = f"""
            current_time=$(date +%H:%M)
            while [[ "$current_time" < "{target_time}" ]]; do
                echo "Waiting for {target_time}, current time: $current_time"
                sleep 60
                current_time=$(date +%H:%M)
            done
            echo "Target time {target_time} reached"
            """
            
            return BashOperator(
                task_id=task_id,
                bash_command=bash_command,
                **kwargs
            )


class ControlMBaseDAG(DAG, ControlMOrderDateMixin, ControlMJobHoldMixin):
    """
    Base DAG class for Control-M migrations.
    
    This class extends Airflow DAG with Control-M specific functionality
    including order date handling and job hold conditions.
    """
    
    def __init__(
        self,
        dag_id: str,
        order_date_strategy: str = "current",
        default_args: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        """
        Initialize Control-M base DAG.
        
        Args:
            dag_id: DAG identifier
            order_date_strategy: Strategy for order date handling
            default_args: Default arguments for tasks
            **kwargs: Additional DAG arguments
        """
        # Set up default arguments
        if default_args is None:
            default_args = {}
        
        # Add Control-M specific defaults
        control_m_defaults = {
            'owner': 'control-m-migration',
            'depends_on_past': False,
            'start_date': datetime(2023, 1, 1),
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        }
        
        # Merge defaults, with provided defaults taking precedence
        merged_defaults = {**control_m_defaults, **default_args}
        
        super().__init__(
            dag_id=dag_id,
            default_args=merged_defaults,
            **kwargs
        )
        
        self.order_date_strategy = order_date_strategy
    
    def get_order_date_for_execution(self, execution_date: datetime) -> str:
        """
        Get order date for a specific execution date.
        
        Args:
            execution_date: Airflow execution date
            
        Returns:
            Order date string
        """
        return self.get_order_date(execution_date, self.order_date_strategy)
    
    def create_control_m_task(
        self,
        task_id: str,
        command: str,
        job_type: str = "command",
        order_date_param: bool = False,
        hold_conditions: Optional[List[Dict[str, Any]]] = None,
        **kwargs
    ) -> BaseOperator:
        """
        Create a task that mimics Control-M job behavior.
        
        Args:
            task_id: Task identifier
            command: Command to execute
            job_type: Type of job ("command", "script", "database")
            order_date_param: Whether to pass order date as parameter
            hold_conditions: List of hold conditions
            **kwargs: Additional task arguments
            
        Returns:
            Airflow operator instance
        """
        # Handle order date parameter
        if order_date_param:
            # Add order date to command if it's a template
            if '{{ order_date }}' in command:
                command = command.replace('{{ order_date }}', '{{ ds }}')
            elif '{{ ORDER_DATE }}' in command:
                command = command.replace('{{ ORDER_DATE }}', '{{ ds }}')
        
        # Create appropriate operator based on job type
        if job_type.lower() == "python":
            from airflow.operators.python import PythonOperator
            
            def python_callable(**context):
                # Execute the Python command
                exec(command)
            
            operator = PythonOperator(
                task_id=task_id,
                python_callable=python_callable,
                **kwargs
            )
        elif job_type.lower() == "database":
            from airflow.operators.sql import SqlOperator
            
            operator = SqlOperator(
                task_id=task_id,
                sql=command,
                **kwargs
            )
        else:
            # Default to BashOperator
            from airflow.operators.bash import BashOperator
            
            operator = BashOperator(
                task_id=task_id,
                bash_command=command,
                **kwargs
            )
        
        # Apply hold conditions if specified
        if hold_conditions:
            hold_tasks = []
            
            for i, hold_condition in enumerate(hold_conditions):
                hold_type = hold_condition.get('type', 'manual')
                hold_value = hold_condition.get('value', '')
                
                hold_task_id = f"{task_id}_hold_{i}"
                
                if hold_type == 'file':
                    hold_task = self.create_file_hold_operator(
                        task_id=hold_task_id,
                        file_path=hold_value
                    )
                elif hold_type == 'time':
                    hold_task = self.create_time_hold_operator(
                        task_id=hold_task_id,
                        target_time=hold_value
                    )
                else:
                    # Manual hold - use DummyOperator
                    from airflow.operators.dummy import DummyOperator
                    
                    hold_task = DummyOperator(
                        task_id=hold_task_id,
                        **kwargs
                    )
                
                hold_tasks.append(hold_task)
            
            # Chain hold conditions before the main task
            if hold_tasks:
                hold_tasks[-1] >> operator
                for i in range(len(hold_tasks) - 1):
                    hold_tasks[i] >> hold_tasks[i + 1]
                
                # Return the first hold task as the entry point
                return hold_tasks[0]
        
        return operator


class ControlMTaskGroup(TaskGroup):
    """
    Task group for organizing Control-M related tasks.
    
    This provides a way to group related tasks together, similar to
    Control-M folder structure.
    """
    
    def __init__(
        self,
        group_id: str,
        order_date_strategy: str = "current",
        **kwargs
    ):
        """
        Initialize Control-M task group.
        
        Args:
            group_id: Group identifier
            order_date_strategy: Order date strategy for tasks in this group
            **kwargs: Additional task group arguments
        """
        super().__init__(group_id=group_id, **kwargs)
        self.order_date_strategy = order_date_strategy


# Utility functions
def create_control_m_dag(
    dag_id: str,
    schedule_interval: str = "@daily",
    start_date: Optional[datetime] = None,
    order_date_strategy: str = "current",
    tags: Optional[List[str]] = None,
    **kwargs
) -> ControlMBaseDAG:
    """
    Utility function to create a Control-M compatible DAG.
    
    Args:
        dag_id: DAG identifier
        schedule_interval: Schedule interval
        start_date: Start date for the DAG
        order_date_strategy: Strategy for order date handling
        tags: List of tags for the DAG
        **kwargs: Additional DAG arguments
        
    Returns:
        ControlMBaseDAG instance
    """
    if start_date is None:
        start_date = datetime(2023, 1, 1)
    
    if tags is None:
        tags = ["control-m-migration"]
    
    return ControlMBaseDAG(
        dag_id=dag_id,
        schedule_interval=schedule_interval,
        start_date=start_date,
        order_date_strategy=order_date_strategy,
        tags=tags,
        **kwargs
    )


def convert_control_m_dependencies(
    tasks: Dict[str, BaseOperator],
    dependencies: List[Dict[str, str]]
) -> None:
    """
    Convert Control-M dependencies to Airflow task dependencies.
    
    Args:
        tasks: Dictionary of task_id -> operator
        dependencies: List of dependency definitions
    """
    for dep in dependencies:
        upstream = dep.get('upstream')
        downstream = dep.get('downstream')
        condition = dep.get('condition', 'success')
        
        if upstream in tasks and downstream in tasks:
            if condition == 'success':
                tasks[upstream] >> tasks[downstream]
            elif condition == 'failure':
                # For failure dependencies, we need to use trigger rules
                tasks[downstream].trigger_rule = 'one_failed'
                tasks[upstream] >> tasks[downstream]
            elif condition == 'completion':
                # For completion dependencies, trigger on success or failure
                tasks[downstream].trigger_rule = 'all_done'
                tasks[upstream] >> tasks[downstream]
        else:
            logger.warning(f"Cannot resolve dependency: {upstream} -> {downstream}")


# Example usage and documentation
"""
Example usage of the Control-M base classes:

```python
from airflow_enterprise_toolkit.dag_base import create_control_m_dag, convert_control_m_dependencies

# Create a Control-M compatible DAG
dag = create_control_m_dag(
    dag_id="control_m_migration_example",
    schedule_interval="@daily",
    order_date_strategy="previous_business_day"
)

# Create tasks with Control-M features
extract_task = dag.create_control_m_task(
    task_id="extract_data",
    command="python3 extract.py --date={{ order_date }}",
    order_date_param=True,
    hold_conditions=[
        {"type": "file", "value": "/data/input.txt"}
    ]
)

transform_task = dag.create_control_m_task(
    task_id="transform_data",
    command="python3 transform.py --input={{ order_date }}_raw.csv"
)

load_task = dag.create_control_m_task(
    task_id="load_data",
    command="python3 load.py --date={{ order_date }}"
)

# Set up dependencies
extract_task >> transform_task >> load_task
```

This example demonstrates:
1. Creating a DAG with Control-M order date handling
2. Creating tasks with order date parameters
3. Adding file hold conditions
4. Setting up task dependencies
"""

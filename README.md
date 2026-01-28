# Airflow Enterprise Toolkit

[![Rust](https://img.shields.io/badge/rust-1.70+-orange.svg)](https://www.rust-lang.org)
[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A comprehensive enterprise toolkit for converting Control-M jobs to Apache Airflow DAGs with support for Control-M specific features like order dates and job hold conditions.

## Features

### Core Functionality
- **Control-M XML Parsing**: Parse Control-M job definitions from XML files
- **Airflow DAG Generation**: Generate equivalent Airflow DAGs with proper task dependencies
- **Order Date Handling**: Support for Control-M order date strategies (current, previous business day, custom offsets)
- **Job Hold Conditions**: Convert Control-M hold conditions (file holds, time holds, resource holds) to Airflow sensors
- **Multiple Job Types**: Support for Command, Script, Database, and File Transfer jobs
- **Clean Architecture**: Professional code structure with clear separation of concerns

### Advanced Features
- **Dependency Management**: Automatic conversion of Control-M job dependencies to Airflow task dependencies
- **Environment Variables**: Preserve and convert Control-M variables to Airflow task configurations
- **Resource Requirements**: Convert Control-M resource specifications to Airflow task settings
- **Validation**: Comprehensive validation of Control-M jobs before conversion
- **Circular Dependency Detection**: Detect and prevent circular dependencies in job definitions

## Architecture

The toolkit follows clean architecture principles with clear layer separation:

```
┌─────────────────┐
│  Presentation   │  ← CLI interface and user interactions
├─────────────────┤
│   Application   │  ← Use cases and business logic
├─────────────────┤
│     Domain      │  ← Core entities and business rules
├─────────────────┤
│ Infrastructure  │  ← XML parsing, file I/O, code generation
└─────────────────┘
```

## Installation

### Prerequisites
- Rust 1.70 or higher
- Python 3.8 or higher
- Apache Airflow 2.0 or higher (for generated DAGs)

### Build from Source

```bash
# Clone the repository
git clone https://github.com/yourusername/airflow-enterprise-toolkit.git
cd airflow-enterprise-toolkit

# Build the Rust CLI tool
cargo build --release

# Install Python dependencies (optional, for base classes)
pip install -r requirements.txt
```

## Quick Start

### 1. Convert Control-M XML to Airflow DAG

```bash
# Basic conversion
./target/release/aet-cli convert --input control_m_jobs.xml --output-name my_dag

# With validation and JSON output
./target/release/aet-cli convert \
    --input control_m_jobs.xml \
    --output-name my_dag \
    --validate \
    --json \
    --include-order-date

# Custom output directory
./target/release/aet-cli convert \
    --input control_m_jobs.xml \
    --output ./generated_dags \
    --output-name my_pipeline
```

### 2. Validate Control-M XML

```bash
# Validate XML file
./target/release/aet-cli validate --input control_m_jobs.xml

# Strict validation mode
./target/release/aet-cli validate --input control_m_jobs.xml --strict
```

### 3. Generate Example Control-M XML

```bash
# Generate basic example
./target/release/aet-cli generate-example --output basic_example.xml --example-type basic

# Generate advanced example with holds and dependencies
./target/release/aet-cli generate-example --output advanced_example.xml --example-type advanced

# Generate hold condition example
./target/release/aet-cli generate-example --output hold_example.xml --example-type hold
```

## Usage Examples

### Control-M XML Example

```xml
<?xml version="1.0" encoding="UTF-8"?>
<CONTROLM>
    <FOLDER>
        <FOLDER_NAME>DataPipeline</FOLDER_NAME>
        <JOB>
            <JOBNAME>DataExtraction</JOBNAME>
            <DESCRIPTION>Extract data from source</DESCRIPTION>
            <COMMAND>python3 extract.py --date={{ORDER_DATE}}</COMMAND>
            <JOB_TYPE>Script</JOB_TYPE>
            <SCHEDULE>
                <DAYS>DAILY</DAYS>
                <ORDER_DATE>PreviousBusinessDay</ORDER_DATE>
            </SCHEDULE>
            <INCOND>
                <NAME>FILE_HOLD:/data/input.txt</NAME>
                <OPERATOR>EQ</OPERATOR>
            </INCOND>
        </JOB>
        <JOB>
            <JOBNAME>DataTransformation</JOBNAME>
            <COMMAND>python3 transform.py --input={{ORDER_DATE}}_raw.csv</COMMAND>
            <JOB_TYPE>Script</JOB_TYPE>
            <INCOND>
                <NAME>DataExtraction</NAME>
                <OPERATOR>EQ</OPERATOR>
            </INCOND>
        </JOB>
    </FOLDER>
</CONTROLM>
```

### Generated Airflow DAG

```python
"""
Generated from Control-M by Airflow Enterprise Toolkit

Original jobs: 2
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': 2023, 1, 1,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'control_m_datapipeline',
    default_args=default_args,
    description='Control-M migration DAG',
    schedule_interval='@daily',
    catchup=False,
    tags=['control-m-migration'],
    max_active_runs=1,
    is_paused=False,
)

# Task definitions
wait_for_file = FileSensor(
    task_id='wait_for_file',
    filepath='/data/input.txt',
    poke_interval=60,
    timeout=3600,
    mode='reschedule',
    dag=dag,
)

data_extraction = BashOperator(
    task_id='data_extraction',
    dag=dag,
    bash_command="python3 extract.py --date={{ ds }}",
)

data_transformation = BashOperator(
    task_id='data_transformation',
    dag=dag,
    bash_command="python3 transform.py --input={{ ds }}_raw.csv",
)

# Task dependencies
wait_for_file >> data_extraction >> data_transformation

# Order date configuration for data_extraction
# Strategy: PreviousBusinessDay
# Template: {{ macros.ds_add(ds, -1) }}
```

## Python Base Classes

The toolkit includes Python base classes for creating Control-M compatible DAGs:

```python
from airflow_enterprise_toolkit.dag_base import create_control_m_dag

# Create a Control-M compatible DAG
dag = create_control_m_dag(
    dag_id="my_control_m_pipeline",
    schedule_interval="@daily",
    order_date_strategy="previous_business_day"
)

# Create tasks with Control-M features
task = dag.create_control_m_task(
    task_id="my_task",
    command="python3 script.py --date={{ order_date }}",
    order_date_param=True,
    hold_conditions=[{"type": "file", "value": "/data/input.txt"}]
)
```

## Configuration

### CLI Configuration

The CLI tool can be configured using command-line arguments:

```bash
./target/release/aet-cli --help
```

Key options:
- `--output`: Output directory for generated files
- `--config`: Configuration file path
- `--verbose`: Enable verbose logging

### Parser Configuration

```rust
use airflow_enterprise_toolkit::infrastructure::parsers::{ControlMParser, ParserConfig};

let config = ParserConfig {
    default_timezone: "UTC".to_string(),
    strict_validation: true,
    default_timeout_minutes: 60,
};

let parser = ControlMParser::with_config(config);
```

### DAG Generation Configuration

```rust
use airflow_enterprise_toolkit::application::services::{DagGenerator, GenerationConfig};

let config = GenerationConfig {
    default_timezone: "UTC".to_string(),
    default_start_date: Utc::now(),
    include_order_date: true,
    order_date_template: "{{ ds }}".to_string(),
    default_tags: vec!["control-m-migration".to_string()],
};

let generator = DagGenerator::with_config(config);
```

## Development

### Running Tests

```bash
# Run Rust tests
cargo test

# Run tests with coverage
cargo test --coverage

# Run Python tests (if available)
python -m pytest tests/
```

### Code Style

The project follows professional coding standards:

- **Rust**: Uses `rustfmt` and `clippy` for code formatting and linting
- **Python**: Follows PEP 8 with type hints and docstrings
- **Documentation**: Comprehensive code comments and API documentation

### Project Structure

```
airflow-enterprise-toolkit/
├── src/
│   ├── domain/           # Core business entities
│   ├── application/      # Use cases and services
│   ├── infrastructure/   # XML parsing, code generation
│   ├── presentation/     # CLI interface
│   └── bin/             # CLI binary
├── python/              # Python base classes
├── examples/            # Example DAGs and XML files
├── tests/               # Integration tests
└── docs/               # Documentation
```

## Control-M Feature Support

### ✅ Supported Features

| Feature | Control-M | Airflow Equivalent | Status |
|---------|-----------|-------------------|---------|
| Order Dates | `ORDER_DATE` | `{{ ds }}`, `ds_add` | ✅ Full |
| File Holds | `FILE_HOLD` | `FileSensor` | ✅ Full |
| Time Holds | `TIME_HOLD` | `TimeSensor` | ✅ Full |
| Job Dependencies | `INCOND` | Task dependencies | ✅ Full |
| Environment Variables | `VARIABLE` | Task environment | ✅ Full |
| Resource Requirements | Timeout, memory | Task configuration | ✅ Full |
| Multiple Job Types | Command, Script, DB | Various operators | ✅ Full |

### 🚧 Partial Support

| Feature | Control-M | Airflow Equivalent | Status |
|---------|-----------|-------------------|---------|
| Complex Schedules | Calendars, holidays | Custom schedules | 🚧 Basic |
| Conditional Logic | IF/THEN conditions | Branching | 🚧 Basic |
| Sub-folders | Folder hierarchy | Task groups | 🚧 Basic |

### ❌ Not Supported

| Feature | Control-M | Airflow Equivalent | Status |
|---------|-----------|-------------------|---------|
| GUI-based configuration | Control-M client | Code-based | ❌ N/A |
| Real-time monitoring | Control-M monitoring | Airflow UI | ❌ Different |
| Job execution engine | Control-M agent | Airflow executor | ❌ Different |

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Follow the existing code style and architecture
- Add comprehensive tests for new features
- Update documentation for API changes
- Ensure all tests pass before submitting PR

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

- **Documentation**: [Full documentation](docs/)
- **Issues**: [GitHub Issues](https://github.com/yourusername/airflow-enterprise-toolkit/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/airflow-enterprise-toolkit/discussions)

## Roadmap

### Version 0.2.0 (Planned)
- [ ] Enhanced scheduling support with calendars
- [ ] More sophisticated hold condition handling
- [ ] Web UI for conversion management
- [ ] Integration with popular databases

### Version 0.3.0 (Planned)
- [ ] Batch conversion capabilities
- [ ] Advanced error handling and recovery
- [ ] Performance optimization for large job sets
- [ ] Plugin system for custom job types

---

**Airflow Enterprise Toolkit** - Professional Control-M to Airflow migration solution.

# RustRover Python Configuration

## Python Interpreter Setup

1. **Virtual Environment Path:**
   ```
   /Users/preedee/Projects/Rust/airflow-enterprise-toolkit/venv/bin/python
   ```

2. **Python Paths to Add:**
   ```
   /Users/preedee/Projects/Rust/airflow-enterprise-toolkit/python
   /Users/preedee/Projects/Rust/airflow-enterprise-toolkit/venv/lib/python3.13/site-packages
   ```

3. **Test Import Command:**
   ```python
   import sys
   sys.path.insert(0, '/Users/preedee/Projects/Rust/airflow-enterprise-toolkit/python')
   import dag_base
   print("✅ Import working!")
   ```

## Verify Installation

```bash
source venv/bin/activate
python -c "import sys; sys.path.insert(0, './python'); import dag_base; print('✅ dag_base imported!')"
```

## Airflow Version

- **Airflow:** 3.1.6
- **Python:** 3.13.11
- **Package:** airflow-enterprise-toolkit (editable install)

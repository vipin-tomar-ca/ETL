# 🚀 Airflow ETL Replacement - Modern SSIS Alternative

## Quick Setup Guide

This project demonstrates how to replace SSIS packages with Apache Airflow for modern, code-first ETL workflows.

## 🎯 What This Replaces

| SSIS Component | Airflow Equivalent | Benefits |
|----------------|-------------------|----------|
| OLE DB Source | PythonOperator + MsSqlHook | Code-first, version control |
| Data Flow Transformations | Python functions + pandas | Better debugging, testing |
| OLE DB Destination | PythonOperator + PostgresHook | Cross-platform, flexible |
| Execute SQL Task | PythonOperator | Dynamic SQL, error handling |
| Send Mail Task | EmailOperator | Built-in, configurable |
| File System Task | BashOperator | Unix commands, scripting |
| Precedence Constraints | Task dependencies | Visual, maintainable |

## 🚀 Quick Start

### 1. Install Airflow
```bash
# Create virtual environment
python -m venv airflow_env
source airflow_env/bin/activate  # On Windows: airflow_env\Scripts\activate

# Install requirements
pip install -r requirements.txt

# Set Airflow home
export AIRFLOW_HOME=~/airflow

# Initialize database
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Start Airflow
airflow webserver --port 8080 &
airflow scheduler &
```

### 2. Configure Connections
```bash
# Add SQL Server connection
airflow connections add 'sql_server_source' \
    --conn-type 'mssql' \
    --conn-host 'localhost' \
    --conn-login 'sa' \
    --conn-password 'your_password' \
    --conn-schema 'your_database'

# Add PostgreSQL connection
airflow connections add 'postgres_target' \
    --conn-type 'postgres' \
    --conn-host 'localhost' \
    --conn-login 'postgres' \
    --conn-password 'your_password' \
    --conn-schema 'your_database'
```

### 3. Deploy DAG
```bash
# Copy DAG to Airflow dags folder
cp etl_pipeline_dag.py ~/airflow/dags/

# Restart Airflow scheduler
pkill -f airflow
airflow scheduler &
```

### 4. Monitor Execution
- Open http://localhost:8080
- Login with admin/admin
- Navigate to DAGs
- Find "etl_pipeline_replacement"
- Trigger or monitor execution

## 🔧 Configuration

### Environment Variables
```bash
export AIRFLOW_HOME=~/airflow
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__SQL_ALCHEMY_CONN=sqlite:///~/airflow/airflow.db
export AIRFLOW__CORE__LOAD_EXAMPLES=False
```

### Airflow Configuration
```ini
# ~/airflow/airflow.cfg
[core]
dags_folder = ~/airflow/dags
load_examples = False
executor = LocalExecutor

[scheduler]
dag_dir_list_interval = 30
max_tis_per_query = 512

[webserver]
web_server_port = 8080
```

## 📊 Monitoring

### Airflow UI
- **DAGs View**: Overview of all workflows
- **Tree View**: Visual execution history
- **Graph View**: Task dependencies
- **Gantt Chart**: Timeline view
- **Code View**: DAG source code

### Logs
```bash
# View task logs
airflow tasks logs etl_pipeline_replacement extract_data 2024-01-01

# View DAG logs
airflow dags backfill etl_pipeline_replacement --start-date 2024-01-01
```

### Metrics
```python
# Custom metrics in tasks
from airflow.models import Variable
import prometheus_client

# Track processing time
start_time = time.time()
# ... processing ...
processing_time = time.time() - start_time

# Store metric
Variable.set("processing_time", processing_time)
```

## 🧪 Testing

### Unit Tests
```python
# test_etl_pipeline.py
import pytest
from airflow.models import DagBag

def test_dag_loaded():
    dagbag = DagBag()
    assert len(dagbag.import_errors) == 0

def test_dag_structure():
    dagbag = DagBag()
    dag = dagbag.get_dag(dag_id='etl_pipeline_replacement')
    assert dag is not None
    assert len(dag.tasks) > 0
```

### Integration Tests
```python
# test_integration.py
def test_extract_transform_load():
    # Test end-to-end pipeline
    # This would test actual database connections
    pass
```

## 🔄 Migration from SSIS

### Step 1: Analyze SSIS Package
```bash
# Document current SSIS workflow
# - Data sources and destinations
# - Transformations and business logic
# - Error handling and notifications
# - Scheduling and dependencies
```

### Step 2: Create Airflow DAG
```python
# Convert SSIS components to Airflow tasks
# - OLE DB Source → PythonOperator with database hook
# - Data Flow → Python functions with pandas
# - Execute SQL → PythonOperator with SQL execution
# - Send Mail → EmailOperator
```

### Step 3: Test and Deploy
```bash
# Test locally
airflow dags test etl_pipeline_replacement 2024-01-01

# Deploy to production
# - Copy DAG to production Airflow instance
# - Configure production connections
# - Set up monitoring and alerting
```

## 🎯 Benefits Over SSIS

### ✅ Developer Experience
- **Code-first**: Version control friendly
- **IDE support**: Full debugging and IntelliSense
- **Testing**: Unit and integration tests
- **Documentation**: Self-documenting code

### ✅ Operations
- **Monitoring**: Rich web UI and metrics
- **Alerting**: Built-in notification system
- **Scaling**: Distributed execution
- **Scheduling**: Flexible cron expressions

### ✅ Maintenance
- **Modular**: Reusable components
- **Debugging**: Detailed logs and error tracking
- **Deployment**: CI/CD friendly
- **Rollback**: Easy version management

## 🚀 Next Steps

1. **Start Simple**: Begin with one SSIS package
2. **Learn Airflow**: Understand DAGs, operators, and hooks
3. **Build Components**: Create reusable transformation functions
4. **Add Monitoring**: Implement custom metrics and alerts
5. **Scale Up**: Migrate more complex packages

## 📚 Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Airflow Providers](https://airflow.apache.org/docs/apache-airflow-providers/)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Airflow Examples](https://github.com/apache/airflow/tree/main/airflow/example_dags)

---

**Ready to modernize your ETL workflows? Start with this template and build your own! 🚀**

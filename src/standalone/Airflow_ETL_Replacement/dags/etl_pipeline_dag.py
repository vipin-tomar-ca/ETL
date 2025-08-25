"""
ETL Pipeline DAG - Apache Airflow replacement for SSIS
This DAG demonstrates how to replace SSIS packages with modern, code-first ETL workflows.
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import logging
import json

# Default arguments for the DAG
default_args = {
    'owner': 'etl-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['etl-team@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create the DAG
dag = DAG(
    'etl_pipeline_replacement',
    default_args=default_args,
    description='Modern ETL Pipeline replacing SSIS packages',
    schedule_interval='0 */6 * * *',  # Run every 6 hours
    max_active_runs=1,
    tags=['etl', 'replacement', 'modern']
)

# Task 1: Extract data from SQL Server (replaces SSIS OLE DB Source)
def extract_data_from_sqlserver(**context):
    """
    Extract data from SQL Server - replaces SSIS OLE DB Source
    """
    try:
        # Get connection details
        mssql_hook = MsSqlHook(mssql_conn_id='sql_server_source')
        
        # Build dynamic query based on execution date
        execution_date = context['execution_date']
        query = f"""
        SELECT 
            CustomerID,
            OrderID,
            OrderDate,
            Amount,
            Status,
            LastModified
        FROM dbo.Orders 
        WHERE LastModified >= '{execution_date.strftime('%Y-%m-%d %H:%M:%S')}'
        AND LastModified < '{execution_date + timedelta(hours=6)}'
        ORDER BY LastModified
        """
        
        # Execute query and get data
        df = mssql_hook.get_pandas_df(sql=query)
        
        # Log extraction results
        logging.info(f"Extracted {len(df)} records from SQL Server")
        
        # Store data in XCom for next task
        context['task_instance'].xcom_push(key='extracted_data', value=df.to_json())
        
        return f"Successfully extracted {len(df)} records"
        
    except Exception as e:
        logging.error(f"Error extracting data: {str(e)}")
        raise

# Task 2: Transform data (replaces SSIS Data Flow Transformations)
def transform_data(**context):
    """
    Transform extracted data - replaces SSIS Data Flow Transformations
    """
    try:
        # Get data from previous task
        ti = context['task_instance']
        data_json = ti.xcom_pull(task_ids='extract_data', key='extracted_data')
        df = pd.read_json(data_json)
        
        logging.info(f"Starting transformation of {len(df)} records")
        
        # Apply transformations (replaces SSIS Derived Column, Conditional Split, etc.)
        
        # 1. Data cleaning (replaces SSIS Derived Column)
        df['Amount'] = df['Amount'].fillna(0)
        df['Status'] = df['Status'].fillna('Unknown')
        
        # 2. Add calculated fields (replaces SSIS Derived Column)
        df['AmountCategory'] = df['Amount'].apply(
            lambda x: 'High' if x > 1000 else 'Medium' if x > 500 else 'Low'
        )
        
        # 3. Add processing metadata
        df['ProcessingTimestamp'] = datetime.now()
        df['ETLJobID'] = context['dag_run'].run_id
        
        # 4. Filter invalid records (replaces SSIS Conditional Split)
        df = df[df['Amount'] >= 0]
        df = df[df['CustomerID'].notna()]
        
        # 5. Data validation (replaces SSIS Row Count)
        invalid_records = len(df[df['Amount'] < 0])
        if invalid_records > 0:
            logging.warning(f"Found {invalid_records} invalid records")
        
        # 6. Aggregate data (replaces SSIS Aggregate)
        customer_summary = df.groupby('CustomerID').agg({
            'OrderID': 'count',
            'Amount': ['sum', 'mean', 'max']
        }).reset_index()
        
        customer_summary.columns = ['CustomerID', 'OrderCount', 'TotalAmount', 'AvgAmount', 'MaxAmount']
        
        # Store transformed data
        context['task_instance'].xcom_push(key='transformed_data', value=df.to_json())
        context['task_instance'].xcom_push(key='customer_summary', value=customer_summary.to_json())
        
        logging.info(f"Transformation completed. {len(df)} records processed, {len(customer_summary)} customers summarized")
        
        return f"Successfully transformed {len(df)} records"
        
    except Exception as e:
        logging.error(f"Error transforming data: {str(e)}")
        raise

# Task 3: Load data to target (replaces SSIS OLE DB Destination)
def load_data_to_target(**context):
    """
    Load transformed data to target database - replaces SSIS OLE DB Destination
    """
    try:
        # Get transformed data
        ti = context['task_instance']
        data_json = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
        summary_json = ti.xcom_pull(task_ids='transform_data', key='customer_summary')
        
        df = pd.read_json(data_json)
        customer_summary = pd.read_json(summary_json)
        
        # Load to target database (PostgreSQL in this example)
        postgres_hook = PostgresHook(postgres_conn_id='postgres_target')
        
        # Load main data
        df.to_sql(
            'orders_processed', 
            postgres_hook.get_sqlalchemy_engine(), 
            if_exists='append', 
            index=False,
            method='multi'
        )
        
        # Load customer summary
        customer_summary.to_sql(
            'customer_summary', 
            postgres_hook.get_sqlalchemy_engine(), 
            if_exists='append', 
            index=False,
            method='multi'
        )
        
        logging.info(f"Successfully loaded {len(df)} records to target database")
        
        return f"Successfully loaded {len(df)} records"
        
    except Exception as e:
        logging.error(f"Error loading data: {str(e)}")
        raise

# Task 4: Update metadata (replaces SSIS Execute SQL Task)
def update_metadata(**context):
    """
    Update processing metadata - replaces SSIS Execute SQL Task
    """
    try:
        # Get processing statistics
        ti = context['task_instance']
        execution_date = context['execution_date']
        
        # Update metadata table
        mssql_hook = MsSqlHook(mssql_conn_id='sql_server_source')
        
        update_query = f"""
        INSERT INTO dbo.ProcessingHistory 
        (JobName, ExecutionDate, Status, RecordsProcessed, ProcessingTime, DagRunID)
        VALUES 
        ('etl_pipeline_replacement', '{execution_date}', 'SUCCESS', 
         (SELECT COUNT(*) FROM dbo.Orders WHERE LastModified >= '{execution_date}'), 
         GETDATE(), '{context["dag_run"].run_id}')
        """
        
        mssql_hook.run(update_query)
        
        logging.info("Metadata updated successfully")
        return "Metadata updated successfully"
        
    except Exception as e:
        logging.error(f"Error updating metadata: {str(e)}")
        raise

# Task 5: Data quality check (replaces SSIS Row Count and validation)
def data_quality_check(**context):
    """
    Perform data quality checks - replaces SSIS validation tasks
    """
    try:
        # Get data for validation
        ti = context['task_instance']
        data_json = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
        df = pd.read_json(data_json)
        
        # Perform quality checks
        quality_checks = {
            'total_records': len(df),
            'null_customer_ids': df['CustomerID'].isnull().sum(),
            'negative_amounts': len(df[df['Amount'] < 0]),
            'duplicate_orders': df.duplicated(subset=['OrderID']).sum(),
            'future_dates': len(df[df['OrderDate'] > datetime.now()])
        }
        
        # Log quality check results
        for check, value in quality_checks.items():
            logging.info(f"Quality check - {check}: {value}")
        
        # Fail if critical issues found
        if quality_checks['null_customer_ids'] > 0:
            raise ValueError(f"Found {quality_checks['null_customer_ids']} records with null CustomerID")
        
        if quality_checks['negative_amounts'] > 0:
            raise ValueError(f"Found {quality_checks['negative_amounts']} records with negative amounts")
        
        # Store quality check results
        context['task_instance'].xcom_push(key='quality_checks', value=json.dumps(quality_checks))
        
        logging.info("Data quality checks passed")
        return "Data quality checks passed"
        
    except Exception as e:
        logging.error(f"Data quality check failed: {str(e)}")
        raise

# Task 6: Send notification (replaces SSIS Send Mail Task)
def send_success_notification(**context):
    """
    Send success notification - replaces SSIS Send Mail Task
    """
    try:
        # Get processing statistics
        ti = context['task_instance']
        quality_checks = json.loads(ti.xcom_pull(task_ids='data_quality_check', key='quality_checks'))
        
        # Build email content
        email_content = f"""
        ETL Pipeline Execution Report
        
        Job: etl_pipeline_replacement
        Execution Date: {context['execution_date']}
        Status: SUCCESS
        
        Processing Statistics:
        - Total Records: {quality_checks['total_records']}
        - Null Customer IDs: {quality_checks['null_customer_ids']}
        - Negative Amounts: {quality_checks['negative_amounts']}
        - Duplicate Orders: {quality_checks['duplicate_orders']}
        - Future Dates: {quality_checks['future_dates']}
        
        All quality checks passed successfully.
        """
        
        # Send email (using Airflow's email operator)
        email_op = EmailOperator(
            task_id='send_email',
            to=['etl-team@company.com'],
            subject=f'ETL Pipeline Success - {context["execution_date"]}',
            html_content=email_content,
            dag=dag
        )
        
        email_op.execute(context)
        
        logging.info("Success notification sent")
        return "Success notification sent"
        
    except Exception as e:
        logging.error(f"Error sending notification: {str(e)}")
        raise

# Task 7: Cleanup temporary data (replaces SSIS File System Task)
def cleanup_temp_data(**context):
    """
    Clean up temporary data - replaces SSIS File System Task
    """
    try:
        # Clean up XCom data older than 7 days
        cleanup_command = """
        # Clean up old XCom entries
        airflow db clean --clean-before-timestamp $(date -d '7 days ago' +%Y-%m-%d)
        
        # Clean up old log files
        find /opt/airflow/logs -name "*.log" -mtime +7 -delete
        """
        
        bash_op = BashOperator(
            task_id='cleanup_bash',
            bash_command=cleanup_command,
            dag=dag
        )
        
        bash_op.execute(context)
        
        logging.info("Cleanup completed successfully")
        return "Cleanup completed successfully"
        
    except Exception as e:
        logging.error(f"Error during cleanup: {str(e)}")
        raise

# Define task dependencies (replaces SSIS precedence constraints)
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data_from_sqlserver,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data_to_target,
    dag=dag
)

metadata_task = PythonOperator(
    task_id='update_metadata',
    python_callable=update_metadata,
    dag=dag
)

quality_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=data_quality_check,
    dag=dag
)

notification_task = PythonOperator(
    task_id='send_notification',
    python_callable=send_success_notification,
    dag=dag
)

cleanup_task = PythonOperator(
    task_id='cleanup_data',
    python_callable=cleanup_temp_data,
    dag=dag
)

# Set up task dependencies (replaces SSIS precedence constraints)
extract_task >> transform_task >> [load_task, quality_task]
load_task >> metadata_task
quality_task >> notification_task
[metadata_task, notification_task] >> cleanup_task

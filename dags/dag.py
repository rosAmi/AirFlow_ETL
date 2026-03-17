# DAG: Directed Acyclic Graph — a collection of tasks that run in a defined order.

"""
Goal:
Build a basic data pipeline using Airflow that:
1. Extracts metadata about top AI models from Hugging Face
2. Transforms/cleans the data
3. Loads it into a PostgreSQL database

This DAG runs locally with Docker and uses open-source tools only.

ETL = Extract, Transform, Load
- Extract: Pull data from source system (Hugging Face API)
- Transform: Clean/process the data (filter duplicates, handle nulls)
- Load: Insert data into target system (PostgreSQL)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import List, Dict, Any
from huggingface_hub import list_models  # used to pull model metadata from Hugging Face

# ==============================
# Step 1: DAG Configuration
# ==============================

# Default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Create the DAG object
dag = DAG(
    'huggingface_model_etl',          # DAG ID
    default_args=default_args,
    description='ETL pipeline for Hugging Face models',
    schedule='@daily',                # Run once a day (equivalent to '0 0 * * *')
    catchup=False,                    # Don’t run for past dates
    tags=['etl', 'huggingface', 'postgres'],
)

# ==============================================
# Step 2: EXTRACT — Fetch data from Hugging Face
# ==============================================

def extract_model_data(**kwargs):
    """
    EXTRACT PHASE: Pull raw model data from the Hugging Face API.
    Returns the top 50 models by download count.
    """

    print("EXTRACT PHASE: Fetching models from Hugging Face Hub...")

    try:
        # Fetch latest 50 models by last modified date
        models = list_models(sort="lastModified", direction=-1, limit=50, cardData=True)

        # Fetch top 50 models by download count
        # models = list_models(sort="downloads", direction=-1, limit=50)

        # Fetch latest models by creation date
        # models = list_models(sort="created", direction=-1, limit=50)

        raw_models = [{
            "model_id": m.id,
            "author": m.author or None,
            "pipeline_tag": m.pipeline_tag or None,
            "tags": m.tags or [],
            "last_modified": m.lastModified,
        } for m in models]

        # Push the raw model data to XCom for the next task
        kwargs['ti'].xcom_push(key='raw_models', value=raw_models)

        print(f"EXTRACT Complete: Retrieved {len(raw_models)} raw model records")
        return "Extract completed successfully"

    except Exception as e:
        print(f"Error in EXTRACT phase: {e}")
        kwargs['ti'].xcom_push(key='raw_models', value=[])
        return "Extract failed"


# Create the Extract task using PythonOperator
extract_task = PythonOperator(
    task_id='extract_huggingface_models',
    python_callable=extract_model_data,
    dag=dag,
)

# ==========================================
# Step 3: TRANSFORM — Clean and process data
# ==========================================
def transform_model_data(**kwargs):
    """
    TRANSFORM PHASE: Clean and process the raw model data.
    - Remove duplicates
    - Handle null values
    - Standardize fields
    """

    ti = kwargs['ti']
    raw_models = ti.xcom_pull(task_ids='extract_huggingface_models', key='raw_models') or []

    print(f"TRANSFORM PHASE: Processing {len(raw_models)} raw records...")

    transformed_data = []
    seen = set()

    for m in raw_models:
        mid = m.get('model_id')
        if not mid or mid in seen:
            continue
        seen.add(mid)

        transformed_data.append({
            "model_id": mid,
            "author": m.get('author') or 'N/A',
            "pipeline_tag": m.get('pipeline_tag') or 'N/A',
            "tags": m.get('tags') or [],
            "last_modified": m.get('last_modified'),
        })

    ti.xcom_push(key='transformed_models', value=transformed_data)

    print(f"TRANSFORM Complete: Produced {len(transformed_data)} cleaned records")
    return "Transform completed successfully"


# Create the Transform task using PythonOperator
transform_task = PythonOperator(
    task_id='transform_model_data',
    python_callable=transform_model_data,
    dag=dag,
)

# ==========================================
# Step 4: LOAD - Insert Data into PostgreSQL
# ==========================================

def load_to_postgres(**kwargs):
    """
    LOAD PHASE: Create table if needed, then insert transformed data into PostgreSQL.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    print("LOAD PHASE: Inserting data into PostgreSQL...")

    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_model_data', key='transformed_models')

    if not transformed_data:
        print("LOAD ERROR: No transformed data available to load.")
        return "No data to load"

    postgres_hook = PostgresHook(postgres_conn_id='models_connection')

    # Create table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS ai_models (
        model_id VARCHAR(255) PRIMARY KEY,
        author VARCHAR(255),
        pipeline_tag VARCHAR(100),
        tags TEXT[],
        last_modified TIMESTAMP
    );
    """

    postgres_hook.run(create_table_query)

    insert_query = """
    INSERT INTO ai_models (model_id, author, pipeline_tag, tags, last_modified)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (model_id) DO UPDATE SET
        author = EXCLUDED.author,
        pipeline_tag = EXCLUDED.pipeline_tag,
        tags = EXCLUDED.tags,
        last_modified = EXCLUDED.last_modified;
    """

    try:
        for model in transformed_data:
            postgres_hook.run(insert_query, parameters=(
                model['model_id'],
                model['author'],
                model['pipeline_tag'],
                model['tags'],
                model['last_modified']
            ))

        print(f"LOAD COMPLETE: Loaded {len(transformed_data)} records")
        return f"Loaded {len(transformed_data)} models"

    except Exception as e:
        print(f"LOAD ERROR: {e}")
        raise


# Create the Load task using PythonOperator
load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

# =============================
# Step 5: ETL Task Dependencies
# =============================

# Set dependencies to create a clear ETL flow:
#   EXTRACT >> TRANSFORM >> LOAD
#            >> SETUP

# Extract must run before Transform
extract_task >> transform_task >> load_task


from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': '2024-10-20',  # More flexible than hardcoding
    'retries': 1,
}

# Defining the DAG
with DAG(
    dag_id='SessionToSnowflake',
    default_args=default_args,
    schedule_interval='@once', # Runs manually
    catchup=False,
) as dag:

    # Task 1: Creating user_session_channel table
    create_user_session_channel_table = SnowflakeOperator(
        task_id='create_user_session_channel_table',
        sql="""
        CREATE TABLE IF NOT EXISTS stock.raw_data.user_session_channel (
            userId int NOT NULL,
            sessionId varchar(32) PRIMARY KEY,
            channel varchar(32) DEFAULT 'direct'
        );
        """,
        snowflake_conn_id='snowflake_conn'
    )

    # Task 2: Creating session_timestamp table
    create_session_timestamp_table = SnowflakeOperator(
        task_id='create_session_timestamp_table',
        sql="""
        CREATE TABLE IF NOT EXISTS stock.raw_data.session_timestamp (
            sessionId varchar(32) PRIMARY KEY,
            ts timestamp
        );
        """,
        snowflake_conn_id='snowflake_conn'
    )

    # Task 3: Creating stage for loading data
    create_stage = SnowflakeOperator(
        task_id='create_stage',
        sql="""
        CREATE OR REPLACE STAGE stock.raw_data.blob_stage
        URL = 's3://s3-geospatial/'
        FILE_FORMAT = (TYPE = csv, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
        """,
        snowflake_conn_id='snowflake_conn'
    )

    # Task 4: Loading data into user_session_channel table
    load_user_session_channel_data = SnowflakeOperator(
        task_id='load_user_session_channel_data',
        sql="""
        COPY INTO stock.raw_data.user_session_channel
        FROM @stock.raw_data.blob_stage/user_session_channel.csv;
        """,
        snowflake_conn_id='snowflake_conn' 
    )

    # Task 5: Loading data into session_timestamp table
    load_session_timestamp_data = SnowflakeOperator(
        task_id='load_session_timestamp_data',
        sql="""
        COPY INTO stock.raw_data.session_timestamp
        FROM @stock.raw_data.blob_stage/session_timestamp.csv;
        """,
        snowflake_conn_id='snowflake_conn' 
    )

    # Setting task dependencies
    create_user_session_channel_table >> create_stage >> load_user_session_channel_data
    create_session_timestamp_table >> create_stage >> load_session_timestamp_data

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

# SQL to create the session_summary table using CTAS with JOIN and duplicate check
session_summary_sql = """
CREATE OR REPLACE TABLE stock.analytics.session_summary AS
SELECT 
    usc.userId,
    usc.sessionId,
    usc.channel,
    st.ts
FROM 
    stock.raw_data.user_session_channel usc
JOIN 
    stock.raw_data.session_timestamp st
ON 
    usc.sessionId = st.sessionId
WHERE
    usc.sessionId IN (
        SELECT 
            sessionId 
        FROM (
            SELECT 
                sessionId, 
                COUNT(*) AS cnt
            FROM 
                stock.raw_data.user_session_channel  -- Corrected the schema here
            GROUP BY 
                sessionId
        ) 
        WHERE cnt = 1  -- Exclude any duplicate sessionIds
    );
"""

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 23),
    'retries': 1,
}

# Defining the DAG
with DAG('session_summary',
         default_args=default_args,
         schedule_interval='@once',  # Run once manually
         catchup=False) as dag:

    # Task to create the session_summary table in Snowflake
    create_session_summary_table = SnowflakeOperator(
        task_id='ctas_session_summary',
        sql=session_summary_sql,  # Updated to use session_summary_sql
        snowflake_conn_id='snowflake_conn',
    )

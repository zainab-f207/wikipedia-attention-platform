import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DBT = "/home/airflow/dbt_venv/bin/dbt"
# Keep dbt's temporary output inside the container, not in the project folder
DBT_ENV = "DBT_TARGET_PATH=/tmp/dbt_target DBT_LOG_PATH=/tmp/dbt_logs"
PROJECT = "/opt/airflow/dbt_attention"


def check_raw_tables():
    import psycopg2

    conn = psycopg2.connect(
        host=os.environ["PGHOST"],
        port=os.environ["PGPORT"],
        dbname="attention",
        user="postgres",
        password=os.environ["POSTGRES_PASSWORD"],
    )
    cur = conn.cursor()
    for table in ["raw.edits", "raw.pageviews_hourly"]:
        cur.execute("select count(*) from " + table)
        rows = cur.fetchone()[0]
        print(table, "has", rows, "rows")
        if rows == 0:
            raise ValueError(table + " is empty")
    conn.close()


with DAG(
    dag_id="attention_pipeline",
    start_date=datetime(2026, 10, 1),
    schedule=None,          # manual: the data is a fixed window, see the README
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["wikipedia", "capstone"],
) as dag:

    check_raw = PythonOperator(
        task_id="check_raw_tables",
        python_callable=check_raw_tables,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd " + PROJECT + " && " + DBT_ENV + " " + DBT + " run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd " + PROJECT + " && " + DBT_ENV + " " + DBT + " test",
    )

    check_raw >> dbt_run >> dbt_test

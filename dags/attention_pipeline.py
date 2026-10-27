import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DBT = "/home/airflow/dbt_venv/bin/dbt"
# Keep dbt's temporary output inside the container, not in the project folder
DBT_ENV = "DBT_TARGET_PATH=/tmp/dbt_target DBT_LOG_PATH=/tmp/dbt_logs"
PROJECT = "/opt/airflow/dbt_attention"
MIN_MATCH_RATE = 0.5   # alert if fewer edited page-hours match a pageview row


def get_conn():
    import psycopg2

    return psycopg2.connect(
        host=os.environ["PGHOST"],
        port=os.environ["PGPORT"],
        dbname="attention",
        user="postgres",
        password=os.environ["POSTGRES_PASSWORD"],
    )


def check_raw_tables():
    conn = get_conn()
    cur = conn.cursor()
    for table in ["raw.edits", "raw.pageviews_hourly"]:
        cur.execute("select count(*) from " + table)
        rows = cur.fetchone()[0]
        print(table, "has", rows, "rows")
        if rows == 0:
            raise ValueError(table + " is empty")
    conn.close()


def one_value(conn, sql):
    # Returns the first value of a query, or None if the query fails
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetchone()[0]
    except Exception:
        conn.rollback()
        return None


def record_health(**context):
    dag_run = context["dag_run"]
    states = {}
    for task_id in ["check_raw_tables", "dbt_run", "dbt_test"]:
        task_instance = dag_run.get_task_instance(task_id)
        states[task_id] = task_instance.state if task_instance else None

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("create schema if not exists monitoring")
    cur.execute("""
        create table if not exists monitoring.pipeline_runs (
            run_id text primary key,
            recorded_at timestamptz default now(),
            status text,
            problems text,
            check_raw_state text,
            dbt_run_state text,
            dbt_test_state text,
            raw_edits bigint,
            raw_pageviews bigint,
            fct_edits_hourly bigint,
            fct_page_attention bigint,
            pageview_match_rate numeric(5, 3),
            latest_edit_hour timestamp
        )
    """)
    conn.commit()

    raw_edits = one_value(conn, "select count(*) from raw.edits")
    raw_pageviews = one_value(conn, "select count(*) from raw.pageviews_hourly")
    fct_edits = one_value(conn, "select count(*) from analytics.fct_edits_hourly")
    fct_attention = one_value(conn, "select count(*) from analytics.fct_page_attention")
    match_rate = one_value(conn, "select avg(has_pageview_row::int) from analytics.fct_page_attention")
    latest_hour = one_value(conn, "select max(edit_hour) from analytics.fct_edits_hourly")

    problems = []
    for task_id, state in states.items():
        if state != "success":
            problems.append(task_id + " is " + str(state))
    for name, value in [("raw_edits", raw_edits), ("raw_pageviews", raw_pageviews),
                        ("fct_edits_hourly", fct_edits), ("fct_page_attention", fct_attention)]:
        if not value:
            problems.append(name + " is empty or missing")
    if match_rate is None or float(match_rate) < MIN_MATCH_RATE:
        problems.append("pageview match rate is " + str(match_rate))

    status = "unhealthy" if problems else "healthy"
    cur.execute(
        """
        insert into monitoring.pipeline_runs
            (run_id, status, problems, check_raw_state, dbt_run_state, dbt_test_state,
             raw_edits, raw_pageviews, fct_edits_hourly, fct_page_attention,
             pageview_match_rate, latest_edit_hour)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (run_id) do update set
            recorded_at = now(), status = excluded.status, problems = excluded.problems,
            check_raw_state = excluded.check_raw_state, dbt_run_state = excluded.dbt_run_state,
            dbt_test_state = excluded.dbt_test_state, raw_edits = excluded.raw_edits,
            raw_pageviews = excluded.raw_pageviews, fct_edits_hourly = excluded.fct_edits_hourly,
            fct_page_attention = excluded.fct_page_attention,
            pageview_match_rate = excluded.pageview_match_rate,
            latest_edit_hour = excluded.latest_edit_hour
        """,
        (dag_run.run_id, status, "; ".join(problems),
         states["check_raw_tables"], states["dbt_run"], states["dbt_test"],
         raw_edits, raw_pageviews, fct_edits, fct_attention, match_rate, latest_hour),
    )
    conn.commit()
    conn.close()
    print("Health report saved:", status, problems)

    # Make the DAG run turn red when something is wrong
    if problems:
        raise ValueError("Pipeline unhealthy: " + "; ".join(problems))


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

    health = PythonOperator(
        task_id="record_health",
        python_callable=record_health,
        trigger_rule="all_done",   # run even if an earlier task failed
        retries=0,
    )

    check_raw >> dbt_run >> dbt_test >> health

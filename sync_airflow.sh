#!/bin/bash
# Copies the DAGs and the dbt project into the Airflow container
export MSYS_NO_PATHCONV=1
docker exec cap_airflow mkdir -p /opt/airflow/dbt_attention
docker cp dags/. cap_airflow:/opt/airflow/dags/
docker cp dbt_attention/. cap_airflow:/opt/airflow/dbt_attention/
docker exec -u root cap_airflow chown -R 50000:0 /opt/airflow/dags /opt/airflow/dbt_attention
docker exec cap_airflow airflow dags reserialize
echo "Sync done"

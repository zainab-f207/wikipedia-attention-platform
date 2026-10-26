#!/bin/bash
export MSYS_NO_PATHCONV=1
docker volume create cap_lake
docker rm -f cap_spark_pv 2>/dev/null

docker create --name cap_spark_pv --user root -v cap_lake:/lake \
  apache/spark:3.5.9-python3 \
  /opt/spark/bin/spark-submit --master "local[2]" --driver-memory 2g /tmp/pageviews_to_parquet.py

docker cp pageviews_to_parquet.py cap_spark_pv:/tmp/pageviews_to_parquet.py
docker cp data/raw cap_spark_pv:/tmp/raw

docker start -a cap_spark_pv
docker rm cap_spark_pv

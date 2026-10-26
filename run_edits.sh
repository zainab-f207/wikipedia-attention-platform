#!/bin/bash
export MSYS_NO_PATHCONV=1
NETWORK=$(docker inspect cap_broker --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')
echo "Using network: $NETWORK"
docker rm -f cap_spark_edits 2>/dev/null

docker create --name cap_spark_edits --user root --network "$NETWORK" \
  -v cap_lake:/lake -v spark_ivy:/ivy \
  apache/spark:3.5.9-python3 \
  /opt/spark/bin/spark-submit --master "local[2]" --driver-memory 1g \
  --conf spark.jars.ivy=/ivy \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 \
  /tmp/edits_to_parquet.py

docker cp edits_to_parquet.py cap_spark_edits:/tmp/edits_to_parquet.py
docker start -a cap_spark_edits
docker rm cap_spark_edits

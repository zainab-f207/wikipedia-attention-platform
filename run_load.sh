#!/bin/bash
export MSYS_NO_PATHCONV=1
NETWORK=$(docker inspect cap_postgres --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')
docker rm -f cap_spark_load 2>/dev/null

docker create --name cap_spark_load --user root --network "$NETWORK" \
  --env-file .env \
  -v cap_lake:/lake -v spark_ivy:/ivy \
  apache/spark:3.5.9-python3 \
  /opt/spark/bin/spark-submit --master "local[2]" --driver-memory 1g \
  --conf spark.jars.ivy=/ivy \
  --packages org.postgresql:postgresql:42.7.4 \
  /tmp/load_to_postgres.py

docker cp load_to_postgres.py cap_spark_load:/tmp/load_to_postgres.py
docker start -a cap_spark_load
docker rm cap_spark_load

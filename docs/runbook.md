# Runbook

Problems seen while building and running this project, with the fix for each.
Windows 11, Git Bash, Docker Desktop (WSL 2). Run `export MSYS_NO_PATHCONV=1`
in every new Git Bash window.

## Start and stop

Memory is tight (Docker is capped at 3 GB in `~/.wslconfig`). Run only what you need.

    docker start cap_postgres              # warehouse, needed by dbt and Airflow
    docker start cap_broker                # only for the edit backfill and run_edits.sh
    docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow
    docker stop cap_airflow cap_broker cap_postgres

## Problems

| Symptom | Cause | Fix |
|---|---|---|
| `fatal error: runtime: cannot allocate memory` from the `docker` command, or Docker keeps turning off | Windows itself is out of memory | `wsl --shutdown`, close big apps, keep `memory=3GB` in `~/.wslconfig` |
| `No space left on device` (pip, Docker) | The C: drive is full; Docker's disk file lives there | Free space on C: (Disk Cleanup, browser cache, `powercfg /hibernate off` as admin), `docker builder prune -f`, then compact `docker_data.vhdx` with `diskpart`. Point pip's temp folder at another drive for big installs |
| `the database system is not yet accepting connections` | Postgres is replaying its log after an unclean stop | Wait 30 to 60 seconds, then check `docker logs --tail 8 cap_postgres` for "ready to accept connections" |
| `UnknownHostException: cap_postgres` in a Spark job | The Postgres container is stopped, so the name does not resolve | `docker start cap_postgres` and run again |
| Python cannot reach `localhost:9092` | The Kafka broker container is stopped | `docker start cap_broker`. Containers use `broker:19092`, the host uses `localhost:9092` |
| `mkdir /run/desktop/mnt/host/e: file exists` or `No such device` in a container | Docker's link to the E: drive went stale | Do not bind-mount E:. Copy files in with `docker cp` (`sync_airflow.sh`), or restart Docker Desktop |
| Paths like `D:/Git/opt/airflow/...` in an error | Git Bash rewrites paths starting with `/` | `export MSYS_NO_PATHCONV=1` |
| Airflow: `Dag id attention_pipeline not found` | The DAG file is not inside the container | `bash sync_airflow.sh`, then trigger again |
| Cannot log in to Airflow | Wrong or forgotten password, or the browser filled in an old one | Use a private window, or `docker exec cap_airflow airflow users reset-password --username zainab --password <new>` |
| Edit backfill finished with far fewer events than expected | Resuming from the last event time skips data, because the replay is not time-ordered | Resume with `Last-Event-ID` (current `produce_edits.py`). Verify with `python check_topic.py`: per-hour counts with no holes, 0 duplicates |
| Spark crashes with `SIGBUS` on a big group-by | Not enough memory | Process one hour at a time and use few shuffle partitions (see `load_to_postgres.py`) |
| `pip install dbt-postgres` picks a beta dbt-core | The version range allows pre-releases | Pin: `pip install "dbt-core==1.11.16" "dbt-postgres==1.11.0"` |
| `record_health` marks a run `unhealthy` | A task failed, a table is empty, or the pageview match rate fell below 0.5 | Read the `problems` column in `monitoring.pipeline_runs`, fix, trigger again |

## Check the pipeline is healthy

    docker exec cap_postgres psql -U postgres -d attention -c "SELECT run_id, status, problems FROM monitoring.pipeline_runs ORDER BY recorded_at DESC LIMIT 5;"

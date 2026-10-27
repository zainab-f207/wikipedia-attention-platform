# Decision log

Each entry: what we decided, why, and what it costs.

## 1. Resume the edit stream with Last-Event-ID, not with event time
The first backfill used the last event's time as the resume point after a
dropped connection. The Wikimedia replay is not strictly time-ordered, so
resuming skipped hours of data: 174,414 events arrived instead of about 500,000.
The fix is to resume from the stream's own position (the `id:` of each event,
sent back as `Last-Event-ID`). Result: 501,604 events, 0 duplicates, no gaps.

## 2. Scope: English Wikipedia, 4 complete hours (12:00-15:59 UTC, 2026-10-22)
Wikimedia only keeps the replay for a limited time and the pipeline runs on a
laptop, so we load 6 hours of edits but analyse only the 4 complete hours.
Hours 11 and 16 are partial and carry `is_complete_hour = false`.

## 3. Flag first, clean later
Staging models keep every row and add flags (`is_article_edit`, `has_colon`,
`is_main_page`, `is_article_candidate`). Only the final tables filter.

## 4. A missing pageview row means 0 views
The hourly pageview file lists only pages that were viewed at least once.
62% of edited page-hours matched a pageview row. Of the other 38%, 74% belong
to pages that do have views in another hour, so titles match. About 10% of all
edited page-hours belong to pages absent from all four hours: either nobody
read them or the titles differ. We cannot tell which, so we count them as 0
views and say so.

## 5. Parquet in a Docker volume instead of MinIO
MinIO's open-source repository is archived, so it is not a safe base for a new
project. The lake is Parquet files partitioned by date and hour in a Docker
volume. At laptop scale this keeps the same layout an object store would have.

## 6. Data minimisation
Usernames are not stored. In the edit stream they can be IP addresses, and the
analysis does not need them.

## 7. Laptop limits
Docker is capped at 3 GB through .wslconfig. Spark jobs run one at a time and
the Kafka broker is stopped when it is not needed. Large Spark aggregations run
one hour at a time because one big group-by crashed the engine.

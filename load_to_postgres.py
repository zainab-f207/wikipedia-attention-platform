import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("load_to_postgres")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

URL = "jdbc:postgresql://cap_postgres:5432/attention"
PASSWORD = os.environ["POSTGRES_PASSWORD"]


def save(df, table, mode):
    (
        df.write.format("jdbc")
        .option("url", URL)
        .option("dbtable", table)
        .option("user", "postgres")
        .option("password", PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", 10000)
        .option("truncate", "true")
        .mode(mode)
        .save()
    )


# 1. English Wikipedia edit events (all namespaces and types, flags kept)
edits = spark.read.parquet("/lake/edits").filter(F.col("wiki") == "enwiki")
edits = edits.select(
    "event_id", "event_ts", "wiki", "namespace", "type",
    "title", "page_title", "is_bot", "byte_change", "is_complete_hour",
)
save(edits, "raw.edits", "overwrite")
print("raw.edits loaded")

# 2. Pageviews, one hour at a time to keep memory small
for hour in [12, 13, 14, 15]:
    pv = spark.read.parquet("/lake/pageviews").filter(F.col("hour") == hour)
    hourly = pv.groupBy("date", "hour", "page_title").agg(
        F.sum(F.when(F.col("domain_code") == "en", F.col("views")).otherwise(0)).alias("views_desktop"),
        F.sum(F.when(F.col("domain_code") == "en.m", F.col("views")).otherwise(0)).alias("views_mobile"),
        F.sum("views").alias("views_total"),
        F.first("has_colon").alias("has_colon"),
        F.first("is_main_page").alias("is_main_page"),
    )
    # The first hour replaces the table, the others add to it
    mode = "overwrite" if hour == 12 else "append"
    save(hourly, "raw.pageviews_hourly", mode)
    print("raw.pageviews_hourly: hour", hour, "loaded")

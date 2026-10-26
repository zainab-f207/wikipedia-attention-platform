from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, LongType

spark = (
    SparkSession.builder.appName("edits_to_parquet")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", StringType()),
    StructField("wiki", StringType()),
    StructField("namespace", IntegerType()),
    StructField("type", StringType()),
    StructField("title", StringType()),
    StructField("is_bot", BooleanType()),
    StructField("byte_change", LongType()),
])

# Batch read: everything in the topic, from the first message to the last
raw = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "broker:19092")
    .option("subscribe", "wiki_changes")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)
df = raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e")).select("e.*")
print("Rows read from Kafka:", df.count())

# Remove duplicates: the same event_id must count once
df = df.dropDuplicates(["event_id"])

df = df.withColumn("event_ts", F.to_timestamp("event_time"))
df = df.withColumn("date", F.to_date("event_ts"))
df = df.withColumn("hour", F.hour("event_ts"))
df = df.withColumn("page_title", F.regexp_replace("title", " ", "_"))
df = df.withColumn("is_complete_hour", F.col("hour").between(12, 15))

df.write.mode("overwrite").partitionBy("date", "hour").parquet("/lake/edits")

back = spark.read.parquet("/lake/edits")
print("Rows written:", back.count())

is_en_article_edit = (
    (F.col("wiki") == "enwiki")
    & (F.col("namespace") == 0)
    & (F.col("type").isin("edit", "new"))
)
back = back.withColumn("en_article_edit", is_en_article_edit)

back.groupBy("hour").agg(
    F.count("*").alias("all_events"),
    F.sum(F.col("en_article_edit").cast("int")).alias("en_article_edits"),
).orderBy("hour").show()

print("Most edited English articles, hours 12 to 15:")
(
    back.filter("en_article_edit AND is_complete_hour")
    .groupBy("page_title").count()
    .orderBy(F.col("count").desc())
    .show(10, truncate=False)
)

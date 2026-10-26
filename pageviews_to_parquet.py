from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("pageviews_to_parquet").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Each line looks like: en Main_Page 213847 0
lines = spark.read.text("/tmp/raw/pageviews-*.gz")
parts = F.split(F.col("value"), " ")
file_name = F.input_file_name()

df = lines.select(
    F.to_date(F.regexp_extract(file_name, r"pageviews-(\d{8})", 1), "yyyyMMdd").alias("date"),
    F.regexp_extract(file_name, r"pageviews-\d{8}-(\d{2})", 1).cast("int").alias("hour"),
    parts.getItem(0).alias("domain_code"),
    parts.getItem(1).alias("page_title"),
    parts.getItem(2).cast("long").alias("views"),
    F.size(parts).alias("n_fields"),
)

# Keep only well-formed lines for English Wikipedia (desktop = en, mobile = en.m)
df = df.filter(F.col("n_fields") == 4)
df = df.filter(F.col("domain_code").isin("en", "en.m"))
df = df.filter(F.col("views").isNotNull() & (F.col("page_title") != ""))

# Flags: we mark rows now and decide what to exclude later (flag first, clean later)
df = df.withColumn("has_colon", F.col("page_title").contains(":"))
df = df.withColumn("is_main_page", F.col("page_title") == "Main_Page")
df = df.drop("n_fields")

df.write.mode("overwrite").partitionBy("date", "hour").parquet("/lake/pageviews")

# Read it back to check what was written
back = spark.read.parquet("/lake/pageviews")
print("Rows written:", back.count())
back.groupBy("hour").agg(F.count("*").alias("rows"), F.sum("views").alias("views")).orderBy("hour").show(24)

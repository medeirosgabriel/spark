from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, substring_index

spark = SparkSession.builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "127.0.0.1") \
    .option("port", 5000) \
    .load()

data = lines.select("value")

data = data.select(
    substring_index("value", " ", 1).alias("datetime"), 
    substring_index("value", " ", -1).alias("price")
)

data = data.select("datetime", "price").dropDuplicates(["datetime"])

query = data \
    .writeStream \
    .queryName("ethereum price") \
    .trigger(processingTime = '1 second') \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
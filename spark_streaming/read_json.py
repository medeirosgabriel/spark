from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# Create a SparkSession
spark = SparkSession.builder.appName("StreamSocketJSON").getOrCreate()

# Define the schema for the incoming JSON data
schema = StructType([
    StructField("Symbol", StringType(), True),
    StructField("Timestamp", LongType(), True),
    StructField("Price", DoubleType(), True),
])

# Set up the streaming source
streaming_data = spark.readStream.format("socket").option("host", "localhost").option("port", 5000).load()

# Parse JSON data
parsed_data = streaming_data.select(from_json(col("value"), schema).alias("json_data")).select("json_data.*")

# Perform operations on the streaming data
#result = parsed_data.groupBy("Symbol").agg({"Price": "avg"})

# Start the streaming query
query = parsed_data.writeStream.outputMode("append").trigger(processingTime='5 seconds').format("console").start()

# Wait for the termination of the streaming query
query.awaitTermination()
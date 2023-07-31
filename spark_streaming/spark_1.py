from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from collections import namedtuple
import time

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext()
ssc = StreamingContext(sc, 1)
sqlContext = SQLContext(sc)

socket_stream = ssc.socketTextStream("127.0.0.1", 5000)
lines = socket_stream

fields = ("crypto", "datetime", "price")
Crypto = namedtuple('Ethereum', fields)

#data = lines.flatMap(lambda text: text.split(" "))
#data = data.map(lambda rec: Crypto("ethereum", rec[0], rec[1]))
#data.foreachRDD(lambda rdd: rdd.toDF().limit(10).registerTempTable("cryptos"))

pairs = lines.flatMap(lambda line: line.split(" "))
pairs.pprint()

ssc.start()
ssc.awaitTermination()


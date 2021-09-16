import findspark

findspark.init()

from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Streaming RDD")
sc = SparkContext.getOrCreate(conf=conf)

ssc = StreamingContext(sc, 1)

rdd = ssc.textFileStream('F:/PySpark Files/Streamng/')

rdd.pprint()

ssc.start()
ssc.awaitTerminationOrTimeout(1000000)
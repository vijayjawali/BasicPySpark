import findspark

findspark.init()

from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("RDD")
sc = SparkContext.getOrCreate(conf=conf)

rdd = sc.textFile('F:/PySpark Files/RDDSample.txt')
print(rdd.collect())

rdd2 = rdd.map(lambda x: x.split(' '))
print(rdd2.collect())

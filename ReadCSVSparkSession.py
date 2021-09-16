import findspark
findspark.init()

from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local[1]").appName("ReadCSV").getOrCreate()
df = spark.read.csv("F:\\PySpark Files\\RDDSample.csv")

df.printSchema()
df.show(10)




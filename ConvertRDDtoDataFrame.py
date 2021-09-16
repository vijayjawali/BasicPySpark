import findspark
findspark.init()

from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf, SparkContext

spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

conf = SparkConf().setAppName("RDD")
sc = SparkContext.getOrCreate(conf=conf)

rdd = sc.textFile('F:/PySpark Files/StudentData.csv')
headers = rdd.first()
rdd = rdd.filter(lambda x: x != headers).map(lambda x: x.split(','))

columns = headers.split(',')
dfRdd: DataFrame = rdd.toDF(columns)
dfRdd.printSchema()
dfRdd.show()




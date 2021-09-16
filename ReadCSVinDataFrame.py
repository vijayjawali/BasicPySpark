import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.master("local[1]").appName("Read CSV in DataFrame").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

schema = StructType([
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("name", StringType(), True),
    StructField("course", StringType(), True),
    StructField("roll", StringType(), True),
    StructField("marks", IntegerType(), True),
    StructField("email", StringType(), True)
])

df = spark.read.options(header=True).schema(schema).csv('F:/PySpark Files/StudentData.csv')
df.printSchema()
df.show(10, False)
spark.stop()

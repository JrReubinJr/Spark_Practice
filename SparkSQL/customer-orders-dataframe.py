from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("CustomerOrderTotals").getOrCreate()

schema = StructType([ \
                     StructField("custID", IntegerType(), True), \
                     StructField("itemID", IntegerType(), True), \
                     StructField("price", FloatType(), True)])

# // Read the file as dataframe
df = spark.read.schema(schema).csv("../res/customer-orders.csv")
df.printSchema()

# GroupBy
customerTotals = df.select('custID', 'price').groupBy("custID").agg(func.round(func.sum("price"),2).alias('totalSpent')).sort('totalSpent')

customerTotals.show(truncate=False)

spark.stop()

                                                  
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("TopProducts").getOrCreate()

pages = spark.read\
    .format("com.databricks.spark.csv")\
    .option("header", "true")\
    .schema('inferSchema')\
    .csv("../res/product_page_pref.csv")

print("Inferred schema:")
pages.printSchema()

no13 = pages.filter(pages.Page.contains('iphone-13')).select('page', 'pageviews')
no13.show(truncate=False)

#








spark.stop()
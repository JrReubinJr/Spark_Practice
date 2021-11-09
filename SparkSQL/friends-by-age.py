from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("Friends-By-Age").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
         .csv("../res/fakefriends-header.csv")

print("Inferred schema:")
people.printSchema()

friendsbyage = people.select("age", "friends").groupBy('age').agg(func.round(func.avg('friends'),2)).sort('age')
friendsbyage.show()






spark.stop()
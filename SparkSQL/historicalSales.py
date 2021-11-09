from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.functions import regexp_extract
from pyspark.sql.types import StructType, StructField, StringType, IntegerType



spark = SparkSession.builder.appName("SaleHistory").getOrCreate()

schema1 = StructType([ \
                     StructField("date", StringType(), True), \
                     StructField("companyID", StringType(), True), \
                     StructField("companyName", StringType(), True), \
                     StructField("sku", StringType(), True), \
                     StructField("description", StringType(), True), \
                     StructField("qty", IntegerType(), True)])

inputDF = spark.read.schema(schema1).csv("../res/historicalOrdersEC.csv")

# schema2 = StructType([ \
#                      StructField("description", StringType(), True), \
#                      StructField("sku", StringType(), True), \
#                      StructField("category", StringType(), True)])

skuDF = spark.read.option('encoding','utf-8').option('inferSchema','true').option('header','true').csv("../res/categories_sku.csv")

YearExp = r'\d{4}-\d{2}'
#MonthExp = r'(?<=\d{4}-)\d{2}'

fixedDateDF = inputDF.select(regexp_extract('date', YearExp, 0).alias('date'),
                             # regexp_extract('date', MonthExp, 0).alias('month'),
                             'companyName',
                             'sku',
                             'qty',
                             'description')

groupedDF = fixedDateDF.groupBy('date','companyName','sku','description').agg(func.sum("qty").alias("qty"))

final = groupedDF.join(skuDF, groupedDF.sku == skuDF.sku,'left').select( groupedDF.date,
                                                                 groupedDF.companyName,
                                                                 skuDF.model,
                                                                 groupedDF.sku,
                                                                 skuDF.category,
                                                                 skuDF.productCategory,
                                                                 groupedDF.qty,
                                                               ).sort(groupedDF.companyName,groupedDF.date)
final.show()
pDF = final.toPandas()
print(pDF)
pDF.to_csv('../res/twEC_results115.csv')
spark.stop()


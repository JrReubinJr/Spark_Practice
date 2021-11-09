from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("OrderTotal")
sc = SparkContext(conf = conf)

orders = sc.textFile("customer-orders.csv")

def parseLine(line):
    fields = line.split(',')
    custID = fields[0]
    amount = float(fields[2])
    return (custID, amount)

processedOrders = orders.map(parseLine).reduceByKey(lambda x,y: round(x+y,2))
sorted = processedOrders.sortBy(lambda x: x[1])

print(sorted.collect())
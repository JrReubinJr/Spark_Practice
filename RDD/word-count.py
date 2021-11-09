from pyspark import SparkConf, SparkContext
import re

def cleanWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("Book.txt")
words = input.flatMap(cleanWords)
wordCounts = words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
sorted = wordCounts.map(lambda x: (x[1],x[0])).sortByKey()

fin =  sorted.collect()
print(fin)
# for res in fin:
#     count = str(res[0])
#     word = res[1]
#     if (word):
#         print(f"{word}: {str(count)}")

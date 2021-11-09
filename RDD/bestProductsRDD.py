from pyspark import SparkConf, SparkContext
import csv

# open the file in the write mode
f = open('../res/bestProductResult.csv', 'w')

# create the csv writer
writer = csv.writer(f)

conf = SparkConf().setMaster("local").setAppName("TopPages")
sc = SparkContext(conf = conf)

csv_file = open('../res/product_page_pref.csv','r')
csv_reader = csv.reader(csv_file)
rows = []
for row in csv_reader:
    rows.append(row)
csv_file.close()

report = sc.parallelize(rows)
print(report.collect())
header = report.first()
report = report.filter(lambda x: x!=header)

def parseLine(line):
    page = line[0].replace('\"','')[11:]
    pageviews = int(line[1].replace(',','').replace('\"','').replace(' ',''))
    return page, pageviews

def extractProduct(line):
    url = line[0].split('/')[1]
    count = line[1]
    return url, count


pages = report.map(parseLine)
pages = pages.filter(lambda x: ('iphone-13') not in x[0])
split = pages.map(extractProduct).reduceByKey(lambda a,b: a+b).sortBy((lambda x: x[1]),ascending=False)
print(split.collect())

for row in split.collect():
    writer.writerow(row)

# close the file
f.close()


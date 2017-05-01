from pyspark import SparkConf, SparkContext
from datetime import datetime

#setting up spark conf
conf = SparkConf().setMaster("local[*]").setAppName("FriendsByAge")
sc = SparkContext(conf= conf)

#caching the RDD so that it can be used multiple times
lines = sc.textFile("purchases_sample.txt")
split_rdd = lines.map(lambda x: x.split('\t')).cache()

#breaking down sales by product category
salesByCategory = split_rdd.map(lambda x: (x[3], float(x[4]))).reduceByKey(lambda x, y: x + y)
sortedResult = salesByCategory.map(lambda x: (x[1], x[0])).sortByKey()
results = sortedResult.collect()

for result in results:
    print(result[1] + " : " + str(result[0]))


#finding the average sales for each day of the week
#parse line function definition

def parseLines(line):
    weekday = datetime.strptime(line[0], "%Y-%m-%d").weekday()
    return(int(weekday) , float(line[1]))

#broadcasting dayDict so that the  0-6 mapping can be done on each worker node
dayDict = {
    0: 'Sunday',
    1: 'Monday',
    2: 'Tuesday',
    3: 'Wednesday',
    4: 'Thursday',
    5: 'Friday',
    6: 'Saturday'
}
dayLookUp = sc.broadcast(dayDict)

salesByDay = split_rdd.map(lambda x: (x[0], x[4]))
weekDaySalesCount = salesByDay.map(parseLines).mapValues(lambda x: (x, 1))

SaleForWeekDay = weekDaySalesCount.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
AvgSaleForWeekDay = SaleForWeekDay.mapValues(lambda x: x[0] / x[1])
mappedResults = AvgSaleForWeekDay.map(lambda x: (dayLookUp.value[x[0]], x[1]))

results = mappedResults.collect()

for result in results:
    print(result)

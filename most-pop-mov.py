from pyspark import SparkConf, SparkContext
#import collections

conf = SparkConf().setMaster("local").setAppName("MostPopularMovie")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///SparkPython/ml-100k/u.data")
movieIdList = lines.map(lambda x: (int(x.split()[1]), 1)).reduceByKey(lambda x, y: x + y)
movieIdFlipSort = movieIdList.map(lambda x: (x[1], x[0])).sortByKey()
results = movieIdFlipSort.collect()

for result in results:
    print(result)





from pyspark import SparkConf, SparkContext

def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.ITEM") as f:
        for lines in f:
            fields = lines.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

conf = SparkConf().setMaster("local").setAppName("Most Pop Movie")
sc = SparkContext(conf = conf)

nameDict = sc.broadcast(loadMovieNames())

lines = sc.textFile("file:///SparkPython/ml-100k/u.data")
movieIdList = lines.map(lambda x : (int(x.split()[1]),1))
movieIdCount = movieIdList.reduceByKey(lambda x, y : x + y)
moviesFlipped = movieIdCount.map(lambda x : (x[1], x[0]))
moviesSorted = moviesFlipped.sortByKey()
moviesWithNames = moviesSorted.map(lambda (count, movie) : (nameDict.value[movie], count))

results = moviesWithNames.collect()
for popMovie in results:
    print(popMovie)
    


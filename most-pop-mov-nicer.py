from pyspark import SparkConf, SparkContext

def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.ITEM") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

conf = SparkConf().setMaster("local").setAppName("PopularMovieName")
sc = SparkContext(conf = conf)

nameDict = sc.broadcast(loadMovieNames())

movieDetails = sc.textFile("file:///sparkpython/ml-100k/u.data")
movieIdList = movieDetails.map(lambda x : (int(x.split()[1]),1))
movieIdCount = movieIdList.reduceByKey(lambda x, y : x + y)
movieIdFlipSort = movieIdCount.map(lambda x : (x[1], x[0])).sortByKey()

moviesWithNames = movieIdFlipSort.map(lambda (count, movie) : (nameDict.value[movie], count))

results = moviesWithNames.collect()
for popularMovieList in results:
    print(popularMovieList)
    

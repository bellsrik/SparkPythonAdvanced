from pyspark import SparkConf, SparkContext

def parseLine1(line1):
    fields = line1.split()
    superheroId = int(fields[0])
    CoOccurenceCount = len(fields) - 1
    return(superheroId, CoOccurenceCount)
    
def parseLine2(line2):
    names = line2.split('\"')
    superheroIdty = int(names[0])
    superheroName = names[1].encode("utf8")
    return(superheroIdty, superheroName)

conf = SparkConf().setMaster("local").setAppName("MostPopularSuperHero")
sc = SparkContext(conf = conf)

line2 = sc.textFile("file:///SparkPython/Marvel-Names.txt")
superheroNames = line2.map(parseLine2)

line1 = sc.textFile("file:///SparkPython/Marvel-Graph.txt")
superheroDetails = line1.map(parseLine1).reduceByKey(lambda x, y : x + y)
superheroFlip = superheroDetails.map(lambda x: (x[1], x[0]))
superheroMax = superheroFlip.max()

mostpopsuperheroName = superheroNames.lookup(superheroMax[1])[0]

print mostpopsuperheroName + " is the most popular superhero with " + str(superheroMax[0]) + " Co-appearances."

from pyspark import SparkContext
#from pyspark import SparkConf
if __name__ == "__main__":
#    conf = SparkConf().setAppName("word count").setMaster("local[1]")
#    sc = SparkContext(conf = conf)
    sc = SparkContext("local[1]", "word count")
    sc.setLogLevel("ERROR")
    
    lines = sc.textFile("in/word_count.text")
    
    words = lines.flatMap(lambda line: line.split(" "))

# assert isinstance(words.countByValue, object)
    wordCounts = words.countByValue()
    for word, count in wordCounts.items():
        print("{} : {}".format(word, count))


import sys

sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf
# from commons.utils import Utils

import re


class Utils:
    COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')


def splitComma(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[2])


if __name__ == "__main__":
    conf = SparkConf().setAppName("airports").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    #    airports sc.textFile("./in/airports.text")
    airports = sc.textFile("C:/Users/Seyhun Altunbay/Documents/Python-Spark/python-spark-tutorial/in/airports.text")
    airportsInUSA = airports.filter(lambda line: Utils.COMMA_DELIMITER.split(line)[3] == "\"United States\"")

    airportsNameAndCityNames = airportsInUSA.map(splitComma)
    airportsNameAndCityNames.saveAsTextFile(
        "C:/Users/Seyhun Altunbay/Documents/Python-Spark/python-spark-tutorial/out/airports_in_usa2.text")

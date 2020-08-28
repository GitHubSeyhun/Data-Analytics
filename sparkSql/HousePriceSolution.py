from pyspark.sql import SparkSession

PRICE_SQ_FT = "Price SQ Ft"

if __name__ == "__main__":

    session = SparkSession.builder.appName("HousePriceSolution").master("local[*]").getOrCreate()
    
    realEstate = session.read \
        .option("header","true") \
        .option("inferSchema", value=True) \
        .csv("C:/Users/Seyhun Altunbay/Documents/Python-Spark/python-spark-tutorial/in/RealEstate.csv")

    realEstate.groupBy("Location") \
        .avg(PRICE_SQ_FT) \
        .orderBy("avg(Price SQ FT)") \
        .show()

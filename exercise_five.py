# what is the value after which we should stop offering discount?
import pandas
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
spark = SparkSession.builder.master("local[*]").appName("Datamanipulation").getOrCreate()
data = spark.read.option("header","true").csv(r"C:\Users\yucel\Documents\MY PROJECTS\EU Superstore\Sample - EU Superstore.csv")
print(data.filter(data.Country == 'France').count())
a = data.filter(data['Profit'] <= 0).limit(1)
print(a.toPandas())
# let's see the totl profit by discount bracket, make sure they are ordered by
import pandas
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
spark = SparkSession.builder.master("local[*]").appName("Datamanipulation").getOrCreate()
data = spark.read.option("header","true").csv(r"C:\Users\yucel\Documents\MY PROJECTS\EU Superstore\Sample - EU Superstore.csv")
discount_groupped = data.groupBy(data.Discount > 0.0).agg(sum('profit').alias('total_profit'))
ordered_discount = discount_groupped.orderBy('total_profit',ascending = False)
#DONE
print(ordered_discount.toPandas())
# Use an SQL query to calculate the profit ratio for each country: hint, ratio is sum(profit)/sum(sales)
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
spark = SparkSession.builder.master("local[*]").appName("Datamanipulation").getOrCreate()
data = spark.read.option("header","true").csv(r"C:\Users\yucel\Documents\MY PROJECTS\EU Superstore\Sample - EU Superstore.csv")
data.createOrReplaceTempView('data')
spark.sql('SELECT DISTINCT Country, SUM(Profit)/SUM(Sales) as Profit_Ratio FROM data GROUP BY Country').show()
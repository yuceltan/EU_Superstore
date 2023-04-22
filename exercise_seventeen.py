# is the country with the largest profit ratio, the country with the largest profit?
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
spark = SparkSession.builder.master("local[*]").appName("Datamanipulation").getOrCreate()
data = spark.read.option("header","true").csv(r"C:\Users\yucel\Documents\MY PROJECTS\EU Superstore\Sample - EU Superstore.csv")
data.createOrReplaceTempView('data')
spark.sql("SELECT Country, SUM(Profit)/SUM(Sales) as Profit_Ratio FROM data GROUP BY Country ORDER BY Profit_Ratio DESC LIMIT 1").show()

# calculate the difference between sales and discount value
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
spark = SparkSession.builder.master("local[*]").appName("Datamanipulation").getOrCreate()
data = spark.read.option("header","true").csv(r"C:\Users\yucel\Documents\MY PROJECTS\EU Superstore\Sample - EU Superstore.csv")
data = data.withColumn('Difference', ( data['Sales'] - data['Discount'] ) )
data.select('Sales','Discount','Difference').show()
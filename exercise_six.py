# who are the top 5 most profitable customers
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
spark = SparkSession.builder.master("local[*]").appName("Datamanipulation").getOrCreate()
data = spark.read.option("header","true").csv(r"C:\Users\yucel\Documents\MY PROJECTS\EU Superstore\Sample - EU Superstore.csv")
print(data.filter(data.Country == 'France').count())
groupped_customer = data.groupBy('Customer Name').agg(sum('Profit').alias('total_profit'))
data.groupBy('Customer Name').agg({'Profit':'sum'})
top_five = groupped_customer.orderBy('total_profit',ascending = False).limit(5)
top_five.show()

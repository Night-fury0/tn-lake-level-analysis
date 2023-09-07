from pyspark.sql import SparkSession
from pyspark.sql.functions import year,month,round, col

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("data/*/", inferSchema=True, header=True)

df = df.withColumn("year",year(df['date'])).withColumn("month",month(df["date"]))


print("years with highest average storage")
df.groupBy("year").avg("storage").orderBy("avg(storage)", ascending=False).withColumn("avg(storage)",round(col('avg(storage)'),2)).show()

# days with highest storage in each year - need to include the date also
print("days with highest storage in each year")
df.groupBy("year").max('storage').orderBy('max(storage)', ascending=False).show()

# days with lowest storage in each year - need to include the date
print("days with lowest storage in each year")
df.groupBy("year").min('storage').orderBy('min(storage)', ascending=False).show()

print("days with highest rainfall")
df.select('date','rainfall').orderBy('rainfall',ascending=False).show(20)

print("years with highest total rainfall")
df.groupBy("year").sum("rainfall").orderBy("sum(rainfall)",ascending=False).withColumn("sum(rainfall)",round(col("sum(rainfall)"),2)).show()

print("top 20 days with highest inflow")
df.select("date","inflow").orderBy("inflow",ascending=False).show(20)

print("top 20 days with highest outflow")
df.select("date","outflow").orderBy("outflow",ascending=False).show(20)

# no of -1




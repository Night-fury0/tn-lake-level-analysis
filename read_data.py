from pyspark.sql import SparkSession
from pyspark.sql.functions import year,month,round,col

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("data/*/", inferSchema=True, header=True)

df = df.withColumn("year",year(df['date'])).withColumn("month",month(df["date"]))

# change based to 'level' to see level stats
based = 'storage'


print(f"years with highest average {based}")
df.groupBy("year").avg(based).orderBy(f"avg({based})", ascending=False).withColumn(f"avg({based})",round(col(f'avg({based})'),2)).show()

print(f"days with highest {based} in each year")
df1 = df.groupBy('year').max(based).withColumnRenamed('year','year1')
df.join(df1,(df[based]==df1[f'max({based})']) & (df['year'] == df1['year1'])).groupBy('year').agg({'date':'min', based:'max'}).orderBy(f'max({based})',ascending=False).show()

print(f"days with lowest {based} in each year")
df1 = df.groupBy('year').min(based).withColumnRenamed('year','year1')
df.join(df1,(df[based]==df1[f'min({based})']) & (df['year'] == df1['year1'])).groupBy('year').agg({'date':'min', based:'min'}).orderBy(f'min({based})',ascending=False).show()

print("days with highest rainfall")
df.select('date','rainfall').orderBy('rainfall',ascending=False).show(20)

print("years with highest total rainfall")
df.groupBy("year").sum("rainfall").orderBy("sum(rainfall)",ascending=False).withColumn("sum(rainfall)",round(col("sum(rainfall)"),2)).show()

print("top 20 days with highest inflow")
df.select("date","inflow").orderBy("inflow",ascending=False).show(20)

print("top 20 days with highest outflow")
df.select("date","outflow").orderBy("outflow",ascending=False).show(20)





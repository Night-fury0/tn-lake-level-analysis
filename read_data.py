from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.option("header",True).option("inferSchema",True).csv("data/*/")

# days with highest rainfall
print("days with highest rainfall")
df.select('date','rainfall').orderBy('rainfall',ascending=False).show()

# days with highest storage
print("days with highest storage")
df.select('date','storage').orderBy('storage',ascending=False).show()

# days with highest storage
print("days with highest level")
df.select('date','level').orderBy('level',ascending=False).show()

# no of -1




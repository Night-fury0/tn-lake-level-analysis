import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType, StructType, StructField,Row
import requests
import datetime


def date_range(start_date, end_date):
	for n in range(int((end_date-start_date).days)):
		yield start_date + datetime.timedelta(n)

# update year variable to the required year data
year = 2023
start_date = datetime.date(year,1,1)
end_date = datetime.date(year+1,1,1)

dates = [[str(i)] for i in date_range(start_date, end_date)]


def getStorage(date):
	try:
		url = f"https://cmwssb.tn.gov.in/lake-level?date={date}"
		values =  BeautifulSoup(requests.get(url).text, 'html.parser').find_all("table")[-1].find_all("td")
		return Row(
			float(values[43].text.replace("-","0")), 
			float(values[44].text.replace("-","0")), 
			float(values[45].text.replace("-","0")),
			float(values[46].text.replace("-","0")), 
			float(values[47].text.replace("-","0")), 
			float(values[48].text.replace("-","0")) 
		)
	except IndexError:
		return Row(-1,-1,-1,-1,-1,-1)

columns = ['date','level','storage','storage_pct','inflow','outflow','rainfall']
spark = SparkSession.builder.appName('lake').getOrCreate()
df = spark.createDataFrame(dates, ["date"])
schema = StructType(
	[StructField("level",DoubleType(),True),
	StructField("storage",DoubleType(),True),
	StructField("storage_pct",DoubleType(),True),
	StructField("inflow",DoubleType(),True),
	StructField("outflow",DoubleType(),True),
	StructField("rainfall",DoubleType(),True)]
)
get_data = udf(getStorage, schema)
df = df.withColumn("Result",get_data(df["date"]))
df = df.select("date","Result.*")
#df.show()
df.write.mode("overwrite").option("header",True).csv(f"data/{year}")

input("Press key to close...")
spark.stop()



# to create a spark dataframe from a html table
#html_stream = StringIO(str(table))
#df = pd.read_html(htmli_stream)[0]
#print(df)

import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
from pyspark.sql import SparkSession
import requests
import datetime

def date_range(start_date, end_date):
	for n in range(int((end_date-start_date).days)):
		yield start_date + datetime.timedelta(n)


#date = "2023-09-05"
year = 2022
start_date = datetime.date(year, 1,1)
end_date = datetime.date(year,1,10)

dates = [[str(i)] for i in date_range(start_date, end_date)]


def getStorage(date):
	url = f"https://cmwssb.tn.gov.in/lake-level?date={date}"
	request=  requests.get(url)
	soup = BeautifulSoup(request.text, 'html.parser')
	table = soup.find_all("table")[-1]
	values = table.find_all("td")
	#html_stream = StringIO(str(table))
	#df = pd.read_html(htmli_stream)[0]
	return tuple([date, values[44].text, values[45].text,values[46].text, values[47].text,values[48].text])
	#print(df)

columns = ['date','storage','storage_pct','inflow','outflow','rainfall']
spark = SparkSession.builder.appName('demo').getOrCreate()
df = spark.createDataFrame(dates, ["date"])
df = df.rdd.map( lambda row : getStorage(row[0]) ).toDF(columns)

df.write.csv("results")

#lake = spark.createDataFrame(df)
#lake.show()

#print(dir(lake))


#emptyRDD = spark.sparkContext.emptyRDD()
#print(emptyRDD)

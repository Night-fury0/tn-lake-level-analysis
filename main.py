import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
from pyspark.sql import SparkSession
import requests

date = "2023-09-05"
url = f"https://cmwssb.tn.gov.in/lake-level?date={date}"

request=  requests.get(url)

soup = BeautifulSoup(request.text, 'html.parser')

table = soup.find_all("table")[-1]

html_stream = StringIO(str(table))
df = pd.read_html(html_stream)[0]

#print(df)

spark = SparkSession.builder.appName('demo').getOrCreate()
lake = spark.createDataFrame(df)
lake.show()




#emptyRDD = spark.sparkContext.emptyRDD()
#print(emptyRDD)

from operator import truediv
from os import system, name
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.functions import substring
import csv
import pandas
from difflib import SequenceMatcher

filepath = "YOUR FILE PATH HERE"
spark = SparkSession.builder.master("local").appName("myRDD").getOrCreate()

spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext

rdd1 = spark.read.option("header", False).option("inferSchema", True).csv(filepath)

####################################################################################
#                                 Variable setup                                   #
####################################################################################

df1 = rdd1.toDF(
    "order_id",
    "customer_id",
    "customer_name",
    "product_id",
    "product_name",
    "product_category",
    "payment_type",
    "qty",
    "price",
    "datetime",
    "country",
    "city",
    "e_commerce_website_name",
    "payment_tnx_id",
    "payment_tnx_success",
    "failure_reason",
)

df1.createOrReplaceTempView("data")

####################################################################################
#                                 Filtering "null"                                 #
####################################################################################
# count of product_id = 109
df_id_cnt = spark.sql('SELECT DISTINCT product_id FROM data WHERE product_id != \'null\'')
# count of product_name = 109
df_name_cnt = spark.sql('SELECT DISTINCT product_name FROM data WHERE product_name != \'null\'')
# count of product_category = 3
df_category_cnt = spark.sql('SELECT DISTINCT product_category FROM data WHERE product_category != \'null\'')
# count of distinct product_price = 109 (verified by comparing different qty of crossbows, which are recorded as same price, so price is base-price, not total)
df_price_cnt = spark.sql('SELECT COUNT(DISTINCT price) FROM data WHERE price != \'null\'')
df_price_cnt.show()

####################################################################################
#                                 Fixing datetime                                  #
####################################################################################
# This function, courtesy of Jed, checks the length of the datetime strings:
# The date has 10 characters, time has 8, and in some instances where they were mashed together, 18.
# Then it returns the datetime to the user in the same format.
def Mydatetimeformatter(inputstring):
    # Date first then time
    arr = str(inputstring["datetime"]).split(" ")
    strdt = ""
    if len(arr[0]) == 8:
        if ":" in arr[0][:2]:
            arr[0] = f"0{arr[0]}"
        strdt = arr[1] + " " + arr[0]
    elif len(arr[0]) == 10:
        if ":" in arr[1][:2]:
            arr[1] = f"0{arr[1]}"
        strdt = arr[0] + " " + arr[1]
    elif len(arr[0]) == 1:
        strdt = arr[0]
    elif len(arr[0]) == 18:

        strdt = arr[0][0:10] + " " + arr[0][10:18]
        # print (strdt)

    return strdt


rdd2 = df1.rdd.map(
    lambda x: (
        x["order_id"],
        x["customer_id"],
        x["customer_name"],
        x["product_id"],
        x["product_name"],
        x["product_category"],
        x["payment_type"],
        x["qty"],
        x["price"],
        Mydatetimeformatter(x),
        x["country"],
        x["city"],
        x["ecommerce_website_name"],
        x["payment_txn_id"],
        x["payment_txn_success"],
        x["failure_reason"],
    )
).toDF()

####################################################################################
#                                 Autocorrecting countries                         #
####################################################################################
corrected_CountryNames = {
    "Angola",
    "Argentina",
    "Australia",
    "Austria",
    "Bangladesh",
    "Bolivia",
    "Brazil",
    "Cote d'Ivoire",
    "Cote d'Ivoire",
    "Canada",
    "China",
    "Chile",
    "China",
    "Colombia",
    "Congo (Kinshasa)",
    "Egypt",
    "France",
    "Greece",
    "India",
    "Indonesia",
    "Iran",
    "Iraq",
    "Japan",
    "Japan",
    "Kazakhstan",
    "Kenya",
    "Kuwait",
    "Malaysia",
    "Mali",
    "Mexico",
    "Mongolia",
    "Morocco",
    "Nigeria",
    "Philippines",
    "Pakistan",
    "Peru",
    "Philippines",
    "Poland",
    "Russia",
    "Russia",
    "Saudi Arabia",
    "Senegal",
    "South Korea",
    "Sudan",
    "Tanzania",
    "Thailand",
    "Togo",
    "Turkey",
    "United Kingdom",
    "United States",
    "Uzbekistan",
    "Vietnam",
    "Malaysia",
}
def get_most_similar(word,wordlist):
     top_similarity = 0.0
     most_similar_word = word
     for candidate in wordlist:
         similarity = SequenceMatcher(None,word,candidate).ratio()
         if similarity > top_similarity:
             top_similarity = similarity
             most_similar_word = candidate
     return most_similar_word

transformed = df1.rdd.map(lambda x : (x["order_id"],x["customer_id"],x["customer_name"],x["product_id"], \
    x["product_name"],x["product_category"],x["payment_type"],x["qty"],x["price"],x["datetime"],get_most_similar(x["country"],corrected_CountryNames),\
    x["city"],x["e_commerce_website_name"],x["payment_tnx_id"], x["payment_tnx_success"],x["failure_reason"])).toDF()
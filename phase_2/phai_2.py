from operator import contains
from xml.dom.minicompat import StringTypes
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_str, substring, expr, when
from pyspark.sql.types import StringType
from datetime import datetime

spark = SparkSession.builder.master("local").appName("data").getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

data_rdd = (
    spark.read.option("header", False)
    .option("inferSchema", True)
    .csv("file:/home/phai597/no_null_casted.csv")
)
rdd1 = spark.read.option('header',False).option('inferSchema',True).csv("file:/home/phai597/no_null_casted.csv")

data_df = data_rdd.toDF(
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
    "ecommerce_website_name",
    "payment_txn_id",
    "payment_txn_success",
    "failure_reason",
)
def Mydatetimeformatter(inputstring):
    #Date first then time
    arr = str(inputstring['datetime']).split(" ")
    strdt = ""
    if len(arr[0]) == 8:
        if ':' in arr[0][:2]:
            arr[0] =  f"0{arr[0]}"
        strdt = arr[1] + " " + arr[0]
    elif len(arr[0]) == 10:
        if ':' in arr[1][:2]:
            arr[1] = f"0{arr[1]}"
        strdt = arr[0] + " " + arr[1]
    elif len(arr[0]) == 1:
        strdt = arr[0]
    elif len(arr[0])==18:

        
        strdt = arr[0][0:10] + " " + arr[0][10:18]
        #print (strdt)
    
    return strdt

rdd2 = data_df.rdd.map(lambda x :  (x['order_id'],x['customer_id'],x['customer_name'],x['product_id'], \
        x['product_name'],x['product_category'],x['payment_type'], x['qty'], x['price'],Mydatetimeformatter(x) , \
        x['country'], x['city'],x['ecommerce_website_name'], x['payment_txn_id'], x['payment_txn_success'], \
        x['failure_reason'])).toDF()



rdd2.toPandas().to_csv('phase_2/testing.csv', header= False, index = False)


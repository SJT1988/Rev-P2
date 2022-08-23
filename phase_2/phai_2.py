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
#new = data_df
new = data_df.filter(
                        (data_df.datetime[0:20].contains(" ")) &
                        (data_df.datetime[0:9].contains("-"))                                                    
)



new.toPandas().to_csv('phase_2/no_null_casted_datetime_formatted.csv')


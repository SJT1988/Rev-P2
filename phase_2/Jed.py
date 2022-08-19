from operator import truediv
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext

spark = SparkSession.builder \
    .master ("local") \
    .appName("myRDD") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext

rdd1 = spark.read.option('header',False).option('inferSchema',True).csv('file:/home/jed/p2_Team1_Data.csv')
#print(rdd1.show(20))
df1 = rdd1.toDF("order_id","customer_id","customer_name","product_id","product_name","product_category", \
    "payment_type","qty","price","datetime","country","city","e_commerce_website_name","payment_tnx_id", \
    "payment_tnx_success","failure_reason")

df1.createOrReplaceTempView("mydata")

df2 = spark.sql("select count(product_category) as mycount, product_category, country from mydata group by product_category,country order by country asc, mycount desc ")
df2.show(10)
# df2.createOrReplaceTempView("P1")

# df3 = spark.sql("select max(mycount) as mycount from P1 group by country order by country")

# print (df3.show(10))

# df4 = spark.sql("select max(mycount), product_category, country as mycount from P1 group by product_category, country order by country")

# df4.show(10)


#print (rdd1.collect())
# in console type spark-submit pythonfilename.py
spark.stop()

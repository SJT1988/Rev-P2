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
#################################################################################################################
#
#                                   Create a Database
#
#################################################################################################################
# spark.sql("drop database P2")
# spark.sql("create database if not exists P2")
# spark.sql("use P2")
#################################################################################################################
#
#                                   Rading the csv file into a RDD
#
#################################################################################################################
rdd1 = spark.read.option('header',False).option('inferSchema',True).csv('file:/home/jed/p2_Team1_Data.csv')

#################################################################################################################
#
#         Reading the RDD into a Data Frame. By doing so we are adding the headers for the Data Frame
#
#################################################################################################################
df_Q1_1 = rdd1.toDF("order_id","customer_id","customer_name","product_id","product_name","product_category", \
    "payment_type","qty","price","datetime","country","city","e_commerce_website_name","payment_tnx_id", \
    "payment_tnx_success","failure_reason")

#################################################################################################################
#
#  Creating a temporary view of the data frame and saving it. these are temporary and are not commited to memory
#
#################################################################################################################

df_Q1_1.createOrReplaceTempView("mydata")

#################################################################################################################
#
#  Query data for the first question:
#	What is the top selling category of items? Per country?
#
#################################################################################################################

df_Q1_2 = spark.sql("select count(product_category) as mycount, product_category, country from mydata group by product_category,country order by country asc, mycount desc ")
df_Q1_2.show(10) # the show statement diplays the results. within the brackets you can define the number of records you want to see

df_Q1_2.createOrReplaceTempView("P1")


# df_Q1_3 = spark.sql("select mycount as mycount, product_category, country from P1 group by country having \
#     mycount = max(select max(mycount) as mycount from P1 group by country) \
# order by country")

df_Q1_3_1 = spark.sql("select max(mycount) as my_count, product_category, country  from P1 group by country order by country")
#df_Q1_3.createOrReplaceTempView("Q1_Final")
df_Q1_3_1.show(10)
#df_Q1_3_1.createOrReplaceTempView("q1_1") 

#df_n = spark.sql("select * from q1_1")
#df_n.show(10)

# df_Q1_3_2 = spark.sql("select q1_1.mycount, \
#                         q1_1.country, \
#                         P1.product_category from q1_1 inner join p1 on \
#                         (p1.country = q1_1.country) \
#                         inner join p1 on \
#                         (p1.mycount = q1_1.mycount) order by country")
# df_Q1_3_2.show(10)


# df_Q1_3_3 = spark.sql()
# df_Q1_Final = spark.sql("show tables") # this is to see if the tables exists
# df_Q1_Final.show()
#    writing the results to a csv file
#    in here you can directly write the data frame into a csv file 
#    we can also convert this to an RDD and write it into a csv file. However, this requires aditional steps

#df_Q1_Final = spark.sql("select * from q1_final")
#df_Q1_Final.show(10)

print ("CSV Printed")
#                               End of Q1 for P2    
#################################################################################################################
# in console type spark-submit pythonfilename.py
spark.stop()

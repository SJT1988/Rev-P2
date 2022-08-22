from time import strptime
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import datetime as dt

spark = SparkSession.builder \
    .master ("local") \
    .appName("myRDD") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext

def Mydatetimeformatter(inputstring):
    arr = str(inputstring).split(" ")
    DateString = []
    timestring = [] 
    if len(arr[0]) == 8:
        timestring = arr[0].split(":")
    elif len(arr[0]) == 10:
        DateString = arr[0].split(":")
    if len(arr[1]) ==8:
        timestring = arr[0].split(":")
    elif len(arr[1]) ==10:
        DateString = arr[0].split(":")

    strdt = str(DateString[0]) + "-" + str(DateString[1]) + "-" + str(DateString[2]) + \
    " "  + str(timestring[0]) + ":" + str(timestring[1]) + ":" + str(timestring[0])

    return strdt

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
#  Filtering out null records from data
#
#################################################################################################################

df_m = spark.sql("select * from mydata where order_id is not null and customer_id is not null and customer_name is not null and \
    product_id is not null and product_name is not null and product_category is not null and payment_type is not null and \
    qty is not null and price is not null and datetime is not null and country is not null and e_commerce_website_name is not null and \
    payment_tnx_id is not null and payment_tnx_success is not null and failure_reason is not null")

# filtering out null as string 
#df_m.show(10)
df_m.createOrReplaceTempView("mydata")



df_m_1 = spark.sql("select * from mydata where product_id != 'null' and \
product_name != 'null' and product_category != 'null' and \
price !='null'")



df_m_1.write.csv('file:/home/jed/cleaned.csv') # writing the cleaned reasults except for data time formatting


#dt.datetime.strptime('2022-08-22 12:05:05','%Y/%m/%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')


#################################################################################################################
#
#  saving the filtered results
#
#################################################################################################################

df_m_1.createOrReplaceTempView("mydata")

#################################################################################################################
#
#  Query data for the first question:
#	What is the top selling category of items? Per country?
#
#################################################################################################################


df_Q1_2 = spark.sql("select count(product_category) as mycount, product_category, country from mydata group by product_category,country order by country asc, mycount desc ")
df_Q1_2.show(10) # the show statement diplays the results. within the brackets you can define the number of records you want to see

df_Q1_2.createOrReplaceTempView("P1")

'''
########################################################################################################################################
LEARNING

spark sql is slightly different to mysql
in my experince 
In my experience, you cannot execute the same complex mysql queries in pyspark for example for Q1 in mysql you can run the following 
command for a data table with the headings [‘mycoint’, ‘product_category’, ‘country’]

select max(mycount), product_category, country from t1 group by country order by country; 

if you run the above mysql query in pyspark.sql it will warn you that product_category is not an aggregate function or is not within
the group by clause.If you include product_category into the group by clause you will have the same data frame as seen above

So,
I used the following command and did a join

select max(mycount) as mycount_n, country as country_n from P1 group by country order by country 

this give the below result 
+---------+----------+
|mycount_n| country_n|
+---------+----------+
|        7|    Angola|
|       80| Argentina|
|       12| Australia|
|        8|   Austria|
|       93|Bangladesh|
|       11|   Bolivia|
|      199|    Brazil|
|       10|    Canada|
|       10|     Chdna|
|        7|     Chile|

The benefit of providing column aliases is important to prevent column ambiguity in joining columns and later on selecting the
right columns from the joined table 

'''
df_Q1_3_1 = spark.sql("select max(mycount) as mycount_n, country as country_n from P1 group by country order by country")
df_Q1_3_1.show(10) # to display results


df_Q1_3_2 = df_Q1_3_1.join(df_Q1_2,[df_Q1_2.mycount == df_Q1_3_1.mycount_n, df_Q1_2.country == df_Q1_3_1.country_n], 'inner')
df_Q1_3_2.show()
'''

You can observe the benefit of proving different column aliases in here 

'''
df_Q1_3_3 = df_Q1_3_2.select([df_Q1_3_2.mycount, df_Q1_3_2.product_category, df_Q1_3_2.country]).orderBy(df_Q1_3_2.country)
df_Q1_3_3.show()

#    writing the results to a csv file
#    in here you can directly write the data frame into a csv file 
#    we can also convert this to an RDD and write it into a csv file. However, this requires aditional steps

df_Q1_3_3.write.csv('file:/home/jed/out.csv')

print ("CSV Printed")
#                               End of Q1 for P2    
#################################################################################################################
# in console type spark-submit pythonfilename.py



'''

some dummy work






'''



#df_m_1.show(10)
#df_m.printSchema()
spark.stop()

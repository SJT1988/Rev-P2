from time import strptime
import pyspark
from re import search
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
    arr = str(inputstring['datetime']).split(" ")
    strdt = ""
    if len(arr[0]) == 8:
        strdt = arr[1] + " " + arr[0]
    elif len(arr[0]) == 10:
        strdt = arr[0] + " " + arr[1]
    elif len(arr[0]) == 1:
        strdt = arr[0]
    elif len(arr[0])==18:

        
        strdt = arr[0][0:10] + " " + arr[0][11:18]
        print (strdt)
    
    return strdt

def Replace_Nulls(myin):
    if str(myin['product_id']) == 'null' or str(myin['product_name']) == 'null' or str(myin['product_category']) == 'null' or \
        str(myin['price']) == 'null' or str(myin['qty']) == 'null':
        return True
    else: 
        return False

def Check_if_String_is_Numeric (myin):

    price = 0.0
    Qty = 0.0
    try:
        price = float(str(myin['price']))
    except:
        Price = -1.0
    
    try:
        Qty = float(str(myin['qty']))
        #print (Qty)
    except:
        Qty = -1.0

    if str(myin['order_id']).isnumeric() == True and str(myin['customer_id']).isnumeric() == True and \
        str(myin['product_id']).isnumeric() == True and price >-1.0 and Qty >1-.0:
        return False
    else:
        return True

    
def Is_Date_string_complete(myin):
    # St = str(myin['datetime'])    
    # D = St.split(" ")
    # if len(D) == 1: 
    #     return False
    # else:
    #     return True
    return True
def Get_Cleaned_Data(My_Linux_File_Path):

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
    rdd1 = spark.read.option('header',False).option('inferSchema',False).csv(My_Linux_File_Path)

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

#spark.sql('select count(*) from mydata').show()

    '''
    df_m = spark.sql("select * from mydata where order_id is not null and customer_id is not null and customer_name is not null and \
        product_id is not null and product_name is not null and product_category is not null and payment_type is not null and \
        qty is not null and price is not null and datetime is not null and country is not null and e_commerce_website_name is not null and \
        payment_tnx_id is not null and payment_tnx_success is not null and failure_reason is not null")

    in the above query a significant amount of records are eliminated since the failure_reason as null records

    '''
    # adding blank value to failure reason to prevent errors

    rddx = df_Q1_1.rdd.map(lambda x :  (x['order_id'],x['customer_id'],x['customer_name'],x['product_id'], 
        x['product_name'],x['product_category'],x['payment_type'], x['qty'], x['price'],x['datetime'], \
        x['country'], x['city'],x['e_commerce_website_name'], x['payment_tnx_id'], x['payment_tnx_success'],"")  \
        if x['failure_reason'] == None  else (x['order_id'],x['customer_id'],x['customer_name'],x['product_id'], \
        x['product_name'],x['product_category'],x['payment_type'], x['qty'], x['price'],x['datetime'], \
        x['country'], x['city'],x['e_commerce_website_name'], x['payment_tnx_id'], x['payment_tnx_success'], \
        x['failure_reason']))

    rddx_2 = rddx.toDF(["order_id","customer_id","customer_name","product_id","product_name","product_category", \
        "payment_type","qty","price","datetime","country","city","e_commerce_website_name","payment_tnx_id", \
        "payment_tnx_success","failure_reason"])

    # filtering out null as string 

    rddx_3 = rddx_2.rdd.map(lambda x :  ("@","@","@","@","@","@","@","@","@","@","@","@","@","@","@","@")  \
        if Replace_Nulls(x) == True else (x['order_id'],x['customer_id'],x['customer_name'],x['product_id'], \
        x['product_name'],x['product_category'],x['payment_type'], x['qty'], x['price'],x['datetime'], \
        x['country'], x['city'],x['e_commerce_website_name'], x['payment_tnx_id'], x['payment_tnx_success'], \
        x['failure_reason']))

    rddx_4 = rddx_3.toDF(["order_id","customer_id","customer_name","product_id","product_name","product_category", \
        "payment_type","qty","price","datetime","country","city","e_commerce_website_name","payment_tnx_id", \
        "payment_tnx_success","failure_reason"])


    # filtering out letters from intiger columns

    rddx_5 = rddx_4.rdd.map(lambda x :  ("@","@","@","@","@","@","@","@","@","@","@","@","@","@","@","@")  \
        if Check_if_String_is_Numeric(x) == True else (x['order_id'],x['customer_id'],x['customer_name'],x['product_id'], \
        x['product_name'],x['product_category'],x['payment_type'], x['qty'], x['price'],x['datetime'], \
        x['country'], x['city'],x['e_commerce_website_name'], x['payment_tnx_id'], x['payment_tnx_success'], \
        x['failure_reason']))

    rddx_6 = rddx_5.toDF(["order_id","customer_id","customer_name","product_id","product_name","product_category", \
        "payment_type","qty","price","datetime","country","city","e_commerce_website_name","payment_tnx_id", \
        "payment_tnx_success","failure_reason"])


    # # check if date string is complete
    # rddx_7 =  rddx_6.rdd.map(lambda x :  ("@","@","@","@","@","@","@","@","@","@","@","@","@","@","@","@")  \
    #     if Is_Date_string_complete(x) == False else (x['order_id'],x['customer_id'],x['customer_name'],x['product_id'], \
    #     x['product_name'],x['product_category'],x['payment_type'], x['qty'], x['price'],x['datetime'], \
    #     x['country'], x['city'],x['e_commerce_website_name'], x['payment_tnx_id'], x['payment_tnx_success'], \
    #     x['failure_reason']))
    rddx_7 = rddx_6.rdd

    rddx_8 = rddx_7.toDF(["order_id","customer_id","customer_name","product_id","product_name","product_category", \
        "payment_type","qty","price","datetime","country","city","e_commerce_website_name","payment_tnx_id", \
        "payment_tnx_success","failure_reason"])

    # Format DateTime

    rddx_9 = rddx_8.rdd.map(lambda x :  (x['order_id'],x['customer_id'],x['customer_name'],x['product_id'], \
        x['product_name'],x['product_category'],x['payment_type'], x['qty'], x['price'],Mydatetimeformatter(x) , \
        x['country'], x['city'],x['e_commerce_website_name'], x['payment_tnx_id'], x['payment_tnx_success'], \
        x['failure_reason']))

    rddxb = rddx_9.toDF(["order_id","customer_id","customer_name","product_id","product_name","product_category", \
        "payment_type","qty","price","datetime","country","city","e_commerce_website_name","payment_tnx_id", \
        "payment_tnx_success","failure_reason"])

    rddxb.createOrReplaceTempView("mydata")
    rddxb2 = spark.sql('select * from mydata where customer_name !="@"')

    return rddxb2 
    #rddxb2.show()

def main():
    bb = Get_Cleaned_Data('file:/home/jed/p2_Team1_Data.csv')
    #print(rdd.rdd.collect())
    #bb.show(15000)
    bb.write.csv('file:/home/jed/Cleaned') 
    print ("File Printed")

if __name__ == '__main__'  :
    main() 
# rddx.toDF("order_id","customer_id","customer_name","product_id","product_name","product_category", \
#     "payment_type","qty","price","datetime","country","city","e_commerce_website_name","payment_tnx_id", \
#     "payment_tnx_success","failure_reason").show(10)


#df_m_1 = df_m.rdd.mapValues(Replace_Nulls)


# df_m_1 = spark.sql("select * from mydata where product_id != '@' and \
# product_name != 'null' and product_category != '@' and \
# price !='@'")


#rddxb2.write.csv('file:/home/jed/cleaned.csv') # writing the cleaned reasults except for data time formatting


#dt.datetime.strptime('2022-08-22 12:05:05','%Y/%m/%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')


#################################################################################################################
#
#  saving the filtered results
#
#################################################################################################################

#df_m_1.createOrReplaceTempView("mydata")

#################################################################################################################
#
#  Query data for the first question:
#	What is the top selling category of items? Per country?
#
#################################################################################################################


# df_Q1_2 = spark.sql("select count(product_category) as mycount, product_category, country from mydata group by product_category,country order by country asc, mycount desc ")
# df_Q1_2.show(10) # the show statement diplays the results. within the brackets you can define the number of records you want to see

# df_Q1_2.createOrReplaceTempView("P1")

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
# df_Q1_3_1 = spark.sql("select max(mycount) as mycount_n, country as country_n from P1 group by country order by country")
# df_Q1_3_1.show(10) # to display results


# df_Q1_3_2 = df_Q1_3_1.join(df_Q1_2,[df_Q1_2.mycount == df_Q1_3_1.mycount_n, df_Q1_2.country == df_Q1_3_1.country_n], 'inner')
# df_Q1_3_2.show()
'''

You can observe the benefit of proving different column aliases in here 

'''
# df_Q1_3_3 = df_Q1_3_2.select([df_Q1_3_2.mycount, df_Q1_3_2.product_category, df_Q1_3_2.country]).orderBy(df_Q1_3_2.country)
# df_Q1_3_3.show()

#    writing the results to a csv file
#    in here you can directly write the data frame into a csv file 
#    we can also convert this to an RDD and write it into a csv file. However, this requires aditional steps

#df_Q1_3_3.write.csv('file:/home/jed/out.csv')

#print ("CSV Printed")
#                               End of Q1 for P2    
#################################################################################################################
# in console type spark-submit pythonfilename.py



'''

some dummy work






'''



#df_m_1.show(10)
#df_m.printSchema()
spark.stop()

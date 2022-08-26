from ast import expr
from operator import truediv
from os import system, name
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.functions import to_timestamp
from pyspark.sql.window import Window
import csv
import pandas

spark = SparkSession.builder \
    .master ("local") \
    .appName("myRDD") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext

root_path = 'file:/home/strumunix/Rev-P2/'
dir_path = 'phase_2/'
filename = 'p2_Team1_Data.csv'
filepath = root_path + dir_path + filename

# we only run this in Ubuntu anyway...
def clear():
    system('clear')

clear()
print('\n\n\n')


rdd1 = spark.read.option('header',False).option('inferSchema',True).csv(filepath)

df1 = rdd1.toDF("order_id","customer_id","customer_name","product_id","product_name","product_category", \
    "payment_type","qty","price","datetime","country","city","e_commerce_website_name","payment_tnx_id", \
    "payment_tnx_success","failure_reason")
'''
df1.createOrReplaceTempView("data")

# count of product_id = 109
df_id_cnt = spark.sql('SELECT DISTINCT product_id FROM data WHERE product_id != \'null\'')
# count of product_name = 109
df_name_cnt = spark.sql('SELECT DISTINCT product_name FROM data WHERE product_name != \'null\'')
# count of product_category = 3
df_category_cnt = spark.sql('SELECT DISTINCT product_category FROM data WHERE product_category != \'null\'')
# count of distinct product_price = 109 (verified by comparing different qty of crossbows, which are recorded as same price, so price is base-price, not total)
df_price_cnt = spark.sql('SELECT COUNT(DISTINCT price) FROM data WHERE price != \'null\'')
df_price_cnt.show()
'''

#=====================
#=====================
#=====================

#################################################################################################################
#                                   FILTER COLUMNS WITH 'NULL' VALUES (strings)
#################################################################################################################
df_no_null = df1.filter(
    (df1.product_id != 'null') & 
    (df1.product_name != 'null') & 
    (df1.product_category != 'null') & 
    (df1.price != 'null') & 
    (df1.qty != 'null')
)
df_no_null.createOrReplaceTempView("no_null")

#################################################################################################################
#                                   TYPE-CAST
#################################################################################################################
df_typecast = spark.sql('SELECT \
    CAST(order_id AS INT), \
    CAST(customer_id AS INT), \
    customer_name, \
    CAST(product_id AS INT), \
    product_name, \
    product_category, \
    payment_type, \
    CAST(qty AS INT), \
    CAST(price AS DECIMAL()), \
    CAST(datetime AS DATE), \
    country, \
    city, \
    e_commerce_website_name, \
    payment_tnx_id, \
    payment_tnx_success, \
    failure_reason \
    FROM no_null')

#################################################################################################################
#                          VERIFYING TYPE FOR EACH COLUMN (SUCCESS!)
#################################################################################################################
# sample = df_typecast.collect()[0] # sample first record
# for c in sample:
#     print(type(c))

#################################################################################################################
#                          WRITE AS NEW CSV. IT WILL CONTAIN NO 'null' values (DONE)
#################################################################################################################
#df_no_null.write.csv(root_path + dir_path + 'no_null')

#################################################################################################################
#
#                                                RUN QUERIES
#
#################################################################################################################
#
#               1a) What is the top selling category of items?
#
#################################################################################################################
df_category_totals = df_typecast.select('product_category', (df_typecast.qty*df_typecast.price).alias('totals'))
df_category_totals.createOrReplaceTempView('category_totals')
df_sum_of_categories = spark.sql('SELECT product_category, SUM(totals) AS cat_sums FROM category_totals GROUP BY product_category ORDER BY cat_sums DESC')
# df_sum_of_categories.show(truncate=False)
# The maximum record is the first ordered record:
highest_grossing_cat_list = list(df_sum_of_categories.collect()[0])
print(f'1a) The highest grossing product category overall is {highest_grossing_cat_list[0]} with {"{:,}".format(highest_grossing_cat_list[1])}')
print('\n\n\n')

#################################################################################################################
#
#               1b) What is the top selling category of items per country?
#
#################################################################################################################

df_category_totals = df_typecast.select('product_category', (df_typecast.qty*df_typecast.price).alias('totals'), 'country')
df_category_totals.createOrReplaceTempView('category_totals')
df_sum_of_categories = spark.sql('SELECT product_category, SUM(totals) AS cat_sums, country FROM category_totals GROUP BY country, product_category ORDER BY country, cat_sums DESC')
# df_sum_of_categories.show(150)
df_sum_of_categories.createOrReplaceTempView('category_totals')
# use a common table expression (CTE) to filter rows
df_gross_by_cat_by_country = spark.sql('WITH max_categories AS ( \
    SELECT MAX(cat_sums) as max_gross, country \
    FROM category_totals \
    GROUP BY country\
) \
SELECT category_totals.product_category, max_categories.max_gross, category_totals.country FROM category_totals \
INNER JOIN max_categories \
ON max_categories.max_gross = category_totals.cat_sums \
AND max_categories.country = category_totals.country \
ORDER BY max_categories.max_gross DESC;')
print('1b) The highest grossing product categories by country are:')
df_gross_by_cat_by_country.show(100)
print('\n\n\n')

# in console type spark-submit pythonfilename.py
spark.stop()
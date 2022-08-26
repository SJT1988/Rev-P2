################################################################################
# This file contains working for Query 1 where "Top-Selling Category" is
# interpreted as the category with the highest gross sales, which is the sum
# of the product of price and quantity for each transaction, grouped by
# category.
################################################################################

from os import system, name
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master ("local") \
    .appName("myRDD") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext

_filepath = 'file:/home/strumunix/Rev-P2/phase_2/'
_sourcedatafile = 'final_data.csv'

# we only run this in Ubuntu anyway...
def clear():
    system('clear')

clear()
print('\n\n\n')

rdd0 = spark.read.option('header',False).option('inferSchema',True).csv(_filepath + _sourcedatafile)

df0 = rdd0.toDF("order_id","customer_id","customer_name","product_id","product_name","product_category", \
    "payment_type","qty","price","datetime","country","city","e_commerce_website_name","payment_tnx_id", \
    "payment_tnx_success","failure_reason")
df0.createOrReplaceTempView('data')

#################################################################################################################
#                                                   TYPE-CAST
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
    CAST(price AS INT), \
    datetime, \
    country, \
    city, \
    e_commerce_website_name, \
    payment_tnx_id, \
    payment_tnx_success, \
    failure_reason \
    FROM data')

#################################################################################################################
#                                                   RUN QUERIES
#################################################################################################################
#################################################################################################################
#
#               1a) What is the top selling category of items?
#
#################################################################################################################
df_category_totals = df_typecast.select('product_category', (df_typecast.qty*df_typecast.price).alias('totals'))
df_category_totals.createOrReplaceTempView('category_totals')
df_sum_of_categories = spark.sql('SELECT product_category, SUM(totals) AS cat_sums FROM category_totals GROUP BY product_category ORDER BY cat_sums DESC')
df_sum_of_categories.show(truncate=False)
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
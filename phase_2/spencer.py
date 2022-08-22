from operator import truediv
from os import system, name
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
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

df1.createOrReplaceTempView("data")

# df2 = spark.sql('SELECT max( \
#     SELECT count(product_category) as cnt \
#     FROM mydata \
#     GROUP BY country \
#     ) as mx, product_category, country \
#     FROM mydata \
#     GROUP BY country, product_category \
#     ORDER BY mx desc'
#     )
# df2.show(53)
# df1.createOrReplaceTempView("mydata")

# df2 = spark.sql("select count(product_category) as mycount, product_category, country from mydata group by product_category,country order by country asc, mycount desc ")
# df2.show(20)
# df2.createOrReplaceTempView("P1")

# df3 = spark.sql("select max(mycount) as mycount from P1 group by country order by country")

# df3.show(20)

# df4 = spark.sql("select max(mycount), product_category, country as mycount from P1 group by product_category, country order by country")

# df4.show(20)


#print (rdd1.collect())


#==========================
#==========================
#==========================
#==========================

# count of null product_id = 984
# df_nullVals = spark.sql('SELECT count(customer_id) FROM data WHERE customer_id = \"null\"')
# df_nullVals.show()

# count of null product_name = 536
# df_nullVals = spark.sql('SELECT count(customer_name) FROM data WHERE customer_name = \"null\"')
# df_nullVals.show()

# count of null product_category = 50
# df_nullVals = spark.sql('SELECT count(product_category) FROM data WHERE product_category = \"null\"')
# df_nullVals.show()

# count of null price = 520
# df_nullVals = spark.sql('SELECT count(customer_name) FROM data WHERE customer_name = \"null\"')
# df_nullVals.show()

# count of product_id = 109
df_id_cnt = spark.sql('SELECT DISTINCT product_id FROM data WHERE product_id != \'null\'')
# count of product_name = 109
df_name_cnt = spark.sql('SELECT DISTINCT product_name FROM data WHERE product_name != \'null\'')
# count of product_category = 3
df_category_cnt = spark.sql('SELECT DISTINCT product_category FROM data WHERE product_category != \'null\'')
# count of distinct product_price = 109 (verified by comparing different qty of crossbows, which are recorded as same price, so price is base-price, not total)
df_price_cnt = spark.sql('SELECT COUNT(DISTINCT price) FROM data WHERE price != \'null\'')
df_price_cnt.show()

df_product_lookup = spark.sql('SELECT DISTINCT CAST(product_id AS INT) AS iid, product_name, product_category, price \
    FROM data WHERE \
    product_id != \'null\' AND \
    product_name != \'null\' AND \
    product_category != \'null\' AND \
    price != \'null\' \
    ORDER BY iid')

# df_product_lookup.write.csv(root_path + dir_path + 'product_lookup')
# df_product_lookup.createOrReplaceTempView("lookup")

# =================================================
# Make list out of lookup dataframe to iterate through during reconstruction
lookup_list = []
for r in df_product_lookup.collect():
    lookup_list.append(list(r))
for x in lookup_list:
    print(x)

#=====================
#=====================
#=====================
# From the original dataset, select the 4 columns containing null data,
# cast the product_id to int (tricky, because the 'null' value (which is a string)
# will break the whole column, causing no characters to be recorded in that field.
# resolved with CASE statement) and sort by the integer casted product_id (iid).
# This let's will help us fill in null values faster when we do the correction.
'''
df_old_sorted_cols = spark.sql('SELECT CAST( \
    CASE \
        WHEN product_id = \'null\' THEN \'0\' \
        ELSE product_id \
    END \
    AS INT) \
    AS iid, product_name, product_category, price FROM data ORDER BY iid')
df_old_sorted_cols.write.csv(root_path + dir_path + 'old_sorted_cols')
'''
# This file needed to be renamed (to old_sorted_cols.csv) and put in phase_2 folder (DONE).

def lookup_by_id(id: int, null_col: int) -> str:
    return lookup_list[id][null_col]

def lookup_by_name(name: str, null_col: int) -> str:
    row = None
    for record in lookup_list:
        if name in record:
            row = lookup_list.index(record)
            return str(lookup_list[row][null_col])
    print('ERROR: LOOKUP_BY_NAME: NAME NOT FOUND IN LOOKUP LIST')
    return ''

with open(root_path + dir_path + 'old_sorted_cols.csv', 'r', newline='') as rf, open(root_path + dir_path + 'null_null.csv', 'w') as wf:
    csvreader = csv.reader(rf,delimiter=',')
    csvwriter = csv.writer(wf,delimiter=',')
    next(csvreader)
    for line in csvreader:
        old_record = line.split(',')
        for each in old_record:
            each.strip()
        if old_record[0] == 0:
            # use product_name to lookup
            pass
        elif old_record[1] == 'null':
            # use product_id to lookup
    
#     pass

# in console type spark-submit pythonfilename.py
spark.stop()
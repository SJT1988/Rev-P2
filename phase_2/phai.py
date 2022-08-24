from xml.dom.minicompat import StringTypes
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_str, substring
from pyspark.sql.types import StringType

spark = SparkSession.builder.master("local").appName("data").getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

data_rdd = (
    spark.read.option("header", False)
    .option("inferSchema", False)
    .csv("file:/home/phai597/no_null_casted_datetime_formatted.csv")
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

data_sql = data_df.createOrReplaceTempView(
    "data"
)  # Makes an SQL-friendly data table to run SQL queries on

only_time = data_df.withColumn(
    "time", substring("datetime", 12, 2)
)  # Creates a new dataframe that is our normal dataframe, but with a new column 'time' that gives only the hour

only_time_2 = only_time.select(["time", "country"])  # For writing to file purposes

# only_time_2.toPandas().to_csv('phai_test.csv') #COMMAND TO WRITE TO CSV!!!

only_time_view = only_time.createOrReplaceTempView(
    "data_2"
)  # SQL-friendly view of our new table with the 'time' column

count = spark.sql("SELECT product_name, country FROM data ORDER BY country ASC")

highest_location_of_sales = spark.sql(
    "SELECT COUNT(product_name) AS sales, COUNTRY FROM data GROUP BY country ORDER BY sales DESC"
)  # Attempt at number 1.


highest_time_of_sales_by_country = spark.sql(
    "SELECT time, COUNT(time) as output FROM data_2 GROUP BY time ORDER BY output ASC"
)  # Attempt at no 4 - Gets the time from every country, but still needed to group by the datetime in asc order

all_countries = spark.sql("SELECT DISTINCT country FROM data ")


count_by_country = spark.sql(
    "SELECT COUNT(product_name) AS amnt, country FROM data GROUP BY country ORDER BY amnt DESC"
)  # Number 3 complete - displays the graph with all of the countries and how many sales in each.


####################################################################################
#                                                                                  #
#                                                                                  #
#                               QUESTION 4                                         #
#                                                                                  #
#                                                                                  #
#                                                                                  #
####################################################################################



# all = spark.sql("SELECT * FROM data")
# count_by_country.show()
# count.show()
# highest_location_of_sales.show()
# highest_time_of_sales.show()
highest_time_of_sales_by_country.show(14999)
# all.show()
#all_countries.show(500)
spark.stop()

from os import lseek
from xml.dom.minicompat import StringTypes
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_str, substring
from pyspark.sql.types import StringType

#_filepath = 'file:/home/phai597/'
_filepath = 'file:/home/strumunix/Rev-P2/phase_2/'
spark = SparkSession.builder.master("local").appName("data").getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

data_rdd = (
    spark.read.option("header", False)
    .option("inferSchema", False)
    .csv(_filepath+"no_null_casted_datetime_formatted.csv")
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

# only_time_2.write.csv(_filepath + 'phai_test') #COMMAND TO WRITE TO CSV!!!

only_time_view = only_time.createOrReplaceTempView(
    "data_2"
)  # SQL-friendly view of our new table with the 'time' column

count = spark.sql("SELECT product_name, country FROM data ORDER BY country ASC")

highest_location_of_sales = spark.sql(
    "SELECT COUNT(product_name) AS sales, country FROM data GROUP BY country ORDER BY sales DESC"
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

ListCountry_Correct = [
    "Angola",
    "Argentina",
    "Australia",
    "Austria",
    "Bangladesh",
    "Bolivia",
    "Brazil",
    "Cote d'Ivoire",
    "Canada",
    "China",
    "Chile",
    "China",
    "Colombia",
    "Congo (Kinshasa)",
    "Egypt",
    "France",
    "Greece",
    "India",
    "Indonesia",
    "Iran",
    "Iraq",
    "Japan",
    "Japan",
    "Kazakhstan",
    "Kenya",
    "Kuwait",
    "Malaysia",
    "Mali",
    "Mexico",
    "Mongolia",
    "Morocco",
    "Nigeria",
    "Philippines",
    "Pakistan",
    "Peru",
    "Philippines",
    "Poland",
    "Russia",
    "Russia",
    "Saudi Arabia",
    "Senegal",
    "South Korea",
    "Sudan",
    "Tanzania",
    "Thailand",
    "Togo",
    "Turkey",
    "United Kingdom",
    "United States",
    "Uzbekistan",
    "Vietnam",
    "Malaysia",
]  # Borrowed from Jed hehe, you da mvp

df_lst = []
df_lst_2 = []
for i in range(len(ListCountry_Correct)):
    tmp_df = only_time.filter(data_df.country == f"{ListCountry_Correct[i]}")
    tmp_view = tmp_df.createOrReplaceTempView("tmp_view")
    tmp = spark.sql(
        f"SELECT time, COUNT(time) AS cust FROM tmp_view GROUP BY time ORDER BY time ASC"
    )

    tmp_view_2 = tmp_df.createOrReplaceTempView("tmp_view_2")
    tmp_2 = spark.sql(
        "SELECT time, count_time FROM (SELECT time, COUNT(time) AS count_time FROM tmp_view_2 GROUP BY time) WHERE count_time = (SELECT MAX(count_time) FROM (SELECT time, COUNT(time) AS count_time FROM tmp_view_2 GROUP BY time))"
    )
    # df_lst.append(tmp)
    # df_lst[i].show()
    df_lst_2.append(tmp_2)
    print(f"Busiest hour(s) of {ListCountry_Correct[i]}: ")
    df_lst_2[i].show()


# all = spark.sql("SELECT * FROM data")
# count_by_country.show()
# count.show()
# highest_location_of_sales.show()
# highest_time_of_sales.show()
# highest_time_of_sales_by_country.show(14999)
# all.show()
# all_countries.show(500)
spark.stop()

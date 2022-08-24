from os import lseek
from xml.dom.minicompat import StringTypes
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_str, substring
from pyspark.sql.types import StringType

_filepath = 'file:/home/phai597/'
# _filepath = "file:/home/strumunix/Rev-P2/phase_2/"
spark = SparkSession.builder.master("local").appName("data").getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

data_rdd = (
    spark.read.option("header", False)
    .option("inferSchema", False)
    .csv(_filepath + "no_null_casted_datetime_formatted.csv")
)

ListCountry_Correct = [
    "Angola",
    "Argentina",
    "Australia",
    "Austria",
    "Bangladesh",
    "Bolivia",
    "Brazil",
    "Côte d'Ivoire",
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

####################################################################################
#                                                                                  #
#                                                                                  #
#                               QUESTION 3                                         #
#                                                                                  #
#                                                                                  #
#                                                                                  #
####################################################################################

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


    ####################################################################################################################
    #                                                  Answers question 4a.                                            #
    ####################################################################################################################

q4_a = spark.sql(
    "SELECT time, count_time FROM (SELECT time, COUNT(time) AS count_time FROM data_2 GROUP BY time)\
    WHERE count_time = (SELECT MAX(count_time) FROM \
    (SELECT time, COUNT(time) AS count_time FROM data_2 GROUP BY time))"
)
file = open("phase_2/Q4.csv", "w")
file.write("time,count,country\n")
file.write(f"{q4_a.collect()[0]['time']},{q4_a.collect()[0]['count_time']},all countries\n")
# q4.show() #Run this to display answer for 4a.

    ####################################################################################################################
    #                                                  Answers question 4b.                                            #
    ####################################################################################################################

df_lst = []  # List of dataframes displaying data from specific countries.
df_lst_2 = []  # Dataframe that only displays the highest time of sales by country.
for i in range(len(ListCountry_Correct)):
    # Filters out all the countries except the one the loop is currently working on. Main working dataframe for this loop.
    tmp_df = only_time.filter(data_df.country == f"{ListCountry_Correct[i]}")
    ####################################################################################################################
    #          Temprorary dataframe to add to the first df list. Not used for the question but just extra data         #
    ####################################################################################################################
    # tmp_view = tmp_df.createOrReplaceTempView("tmp_view")
    # tmp = spark.sql(
    #     f"SELECT time, COUNT(time) AS cust FROM tmp_view GROUP BY time ORDER BY time ASC"
    # )
    # df_lst.append(tmp)
    # df_lst[i].show()

    tmp_view_2 = tmp_df.createOrReplaceTempView("tmp_view_2")
    q4_b = spark.sql(
        "SELECT time, count_time FROM (SELECT time, COUNT(time) AS count_time FROM tmp_view_2 GROUP BY time)\
        WHERE count_time = (SELECT MAX(count_time) FROM \
        (SELECT time, COUNT(time) AS count_time FROM tmp_view_2 GROUP BY time))"
    )
    df_lst_2.append(q4_b)
    time = df_lst_2[i].collect()[-1]['time']
    freq = df_lst_2[i].collect()[-1]['count_time']
    curr_country = ListCountry_Correct[i]
    file.write(f"{time},{freq},{curr_country}\n")
    #print(f"Busiest hour(s) of {ListCountry_Correct[i]}: ") #Run these 2 to display answer from 4b.
    #df_lst_2[i].show()

file.close()
# all = spark.sql("SELECT * FROM data")
# count_by_country.show()
# count.show()
# highest_location_of_sales.show()
# highest_time_of_sales.show()
# all.show()
spark.stop()

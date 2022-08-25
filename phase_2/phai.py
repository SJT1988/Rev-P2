from os import lseek
from xml.dom.minicompat import StringTypes
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_str, substring
from pyspark.sql.types import StringType

_filepath = "file:/home/phai597/"
# _filepath = "file:/home/strumunix/Rev-P2/phase_2/"
spark = SparkSession.builder.master("local").appName("data").getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

data_rdd = (
    spark.read.option("header", False)
    .option("inferSchema", False)
    .csv(_filepath + "final_data.csv")
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

only_date = data_df.withColumn(
    "month", substring("datetime", 6, 2)
)  # Creates a new dataframe similar to only_time, but with the month of sale

only_month = only_date.createOrReplaceTempView("only_month_view")

only_month_casted = spark.sql(
    "SELECT order_id, customer_id, customer_name, product_id, product_name, product_category, payment_type, qty, price, datetime, country, city, ecommerce_website_name, payment_txn_id, payment_txn_success, failure_reason, CAST(month AS INT) FROM only_month_view"
)


only_month_casted_view = only_month_casted.createOrReplaceTempView("only_month_casted")


only_time_2 = only_time.select(["time", "country"])  # For writing to file purposes

# only_time_2.write.csv(_filepath + 'phai_test') #COMMAND TO WRITE TO CSV!!!

only_time_view = only_time.createOrReplaceTempView(
    "data_2"
)  # SQL-friendly view of our new table with the 'time' column


####################################################################################
#                                                                                  #
#                                                                                  #
#                               QUESTION 2                                         #
#                                                                                  #
#                                                                                  #
#                                                                                  #
####################################################################################

####################################################################################################################
#                                                  Answers question 2a.                                            #
####################################################################################################################
file = open('phase_2/Q2_a.csv', 'w')
file2 = open('phase_2/Q2_b.csv', 'w')
quarter_1 = only_month_casted.filter(only_month_casted.month < 4)  # Months 1-3
quarter_2 = only_month_casted.filter((only_month_casted.month < 7) & (only_month_casted.month > 3)) #Months 4-6
quarter_3 = only_month_casted.filter((only_month_casted.month < 10) & (only_month_casted.month > 6)) #Months 7-9
# quarter_4 = only_date.filter(only_date.month  < 13 & only_date.month > 9) #Months 10-12, but their data only covers months from the first 3 quarters.
quarter_1_view = quarter_1.createOrReplaceTempView("q1")
quarter_2_view = quarter_2.createOrReplaceTempView("q2")
quarter_3_view = quarter_3.createOrReplaceTempView("q3")
# quarter_4_view = quarter_4.createOrReplaceTempView("q4")

pop_q1 = spark.sql(
    "SELECT COUNT(product_name) AS amnt, product_name FROM q1 GROUP BY product_name ORDER BY amnt DESC"
)
pop_q2 = spark.sql(
    "SELECT COUNT(product_name) AS amnt, product_name FROM q2 GROUP BY product_name ORDER BY amnt DESC"
)
pop_q3 = spark.sql(
    "SELECT COUNT(product_name) AS amnt, product_name FROM q3 GROUP BY product_name ORDER BY amnt DESC"
)

for i in range(5):
    amnt = pop_q1.collect()[i]['amnt']
    product = pop_q1.collect()[i]['product_name']
    quarter = "1"
    file.write(f"{amnt},{product},{quarter}\n")
for i in range(5):
    amnt = pop_q2.collect()[i]['amnt']
    product = pop_q2.collect()[i]['product_name']
    quarter = "2"
    file.write(f"{amnt},{product},{quarter}\n")
for i in range(5):
    amnt = pop_q3.collect()[i]['amnt']
    product = pop_q3.collect()[i]['product_name']
    quarter = "3"
    file.write(f"{amnt},{product},{quarter}\n")

# print("Top products overall for first quarter:")
# pop_q1.show(3)
# print("Top products overall for second quarter:")
# pop_q2.show(3)
# print("Top products overall for third quarter:")
# pop_q3.show(3)
####################################################################################################################
#                                                  Answers question 2b.                                            #
####################################################################################################################

for i in range(len(ListCountry_Correct)):
    curr_country = ListCountry_Correct[i]
    quarter_1_country = quarter_1.filter(quarter_1.country == curr_country)
    quarter_2_country = quarter_2.filter(quarter_2.country == curr_country)
    quarter_3_country = quarter_3.filter(quarter_3.country == curr_country)
    quarter_1_country_view = quarter_1_country.createOrReplaceTempView("q1_country")
    quarter_2_country_view = quarter_2_country.createOrReplaceTempView("q2_country")
    quarter_3_country_view = quarter_3_country.createOrReplaceTempView("q3_country")

    pop_q1_country = spark.sql(
        "SELECT COUNT(product_name) AS amnt, product_name FROM q1_country GROUP BY product_name ORDER BY amnt DESC"
    )
    pop_q2_country = spark.sql(
        "SELECT COUNT(product_name) AS amnt, product_name FROM q2_country GROUP BY product_name ORDER BY amnt DESC"
    )
    pop_q3_country = spark.sql(
        "SELECT COUNT(product_name) AS amnt, product_name FROM q3_country GROUP BY product_name ORDER BY amnt DESC"
    )

    # for j in range(5):
    #     if j < len(pop_q1_country.collect()):
    #         amnt = pop_q1_country.collect()[j]['amnt']
    #         product = pop_q1_country.collect()[j]['product_name']
    #         quarter = 1
    #         file2.write(f"{amnt},{product},{quarter},{curr_country}\n")
    # for j in range(5):
    #     if j < len(pop_q2_country.collect()):
    #         amnt = pop_q2_country.collect()[j]['amnt']
    #         product = pop_q2_country.collect()[j]['product_name']
    #         quarter = 2
    #         file2.write(f"{amnt},{product},{quarter},{curr_country}\n")
    # for j in range(5):
    #     if j < len(pop_q3_country.collect()):
    #         amnt = pop_q3_country.collect()[j]['amnt']
    #         product = pop_q3_country.collect()[j]['product_name']
    #         quarter = 3
    #         file2.write(f"{amnt},{product},{quarter},{curr_country}\n")


    # print(f"Q1 data for {curr_country}:")
    # pop_q1_country.show()
    # print(f"Q2 data for {curr_country}:")
    # pop_q2_country.show()
    # print(f"Q3 data for {curr_country}:")
    # pop_q3_country.show()
file.close()
file2.close()
####################################################################################################################
#                                                     End question 2                                               #
####################################################################################################################
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
)

# file = open("phase_2/Q3.csv", "w")
# file.write("sales,country\n")
# for row in count_by_country.collect():
#     file.write(f"{row['amnt']},{row['country']}\n")
# file.close()
####################################################################################################################
#                                                     End question 3                                               #
####################################################################################################################

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

# file = open("phase_2/Q4.csv", "w")
# file.write("time,count,country\n")
# file.write(
#     f"{q4_a.collect()[0]['time']},{q4_a.collect()[0]['count_time']},all countries\n"
# )
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

    # for elem in q4_b.collect():
    #     time = elem['time']
    #     freq = elem['count_time']
    #     curr_country = ListCountry_Correct[i]
    #     file.write(f"{time},{freq},{curr_country}\n")

    # time = df_lst_2[i].collect()[-1]["time"]
    # freq = df_lst_2[i].collect()[-1]["count_time"]
    # curr_country = ListCountry_Correct[i]
    # file.write(f"{time},{freq},{curr_country}\n")
    # print(f"Busiest hour(s) of {ListCountry_Correct[i]}: ") #Run these 2 to display answer from 4b.
    # df_lst_2[i].show()

file.close()
# all = spark.sql("SELECT * FROM data")
# count_by_country.show()
# count.show()
# highest_location_of_sales.show()
# highest_time_of_sales.show()
# all.show()
spark.stop()

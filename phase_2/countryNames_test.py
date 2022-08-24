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

from difflib import SequenceMatcher # Used for incorrect names in column countries

# Try appling through visual studio for csv?



spark = SparkSession.builder \
    .master ("local") \
    .appName("myRDD") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext

# Note need to make directory(ies) Rev-P2 and phase_2
root_path = 'file:/home/khan5203/Rev-P2/'
dir_path = 'phase_2/'
filename = 'p2_Team1_Data.csv'
filepath = root_path + dir_path + filename

# Ubuntu code:
#def clear():
#    system('clear')

#clear()
#print('\n\n\n')

rdd1 = spark.read.option('header',False).option('inferSchema',True).csv(filepath)

df1 = rdd1.toDF("order_id","customer_id","customer_name","product_id","product_name","product_category", \
    "payment_type","qty","price","datetime","country","city","e_commerce_website_name","payment_tnx_id", \
    "payment_tnx_success","failure_reason")

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

# Thank you Spencer for the examples

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
    CAST(price AS INT), \
    CAST(datetime AS DATE), \
    country, \
    city, \
    e_commerce_website_name, \
    payment_tnx_id, \
    payment_tnx_success, \
    failure_reason \
    FROM no_null')
    # Borrowing from Spencer, apologies
###############################################################################################################################
#                                       FILTER COUNTRY NAMES THAT ARE INCORRECT, AND REPLACE?                                 #
###############################################################################################################################

# Countries are located in Col 11 of csv (12 is cities)
# Note: It is listed as "country"
'''
    "Angola","Argentina","Australia","Austria","Bangladesh","Bolivia","Brazil","Cote d'Ivoire","Cote d'Ivoire","Canada","China","Chile",
    "China",
    "Colombia",
    "Congo (Kinshasa)",
    "Egypt",
    "France",
    "Greece",
    "India","Indonesia","Iran","Iraq","Japan","Japan","Kazakhstan","Kenya","Kuwait","Malaysia","Mali","Mexico","Mongolia","Morocco","Nigeria",
    "Philippines",
    "Pakistan",
    "Peru",
    "Philippines","Poland","Russia","Russia","Saudi Arabia","Senegal",
    "South Korea",
    "Sudan",
    "Tanzania",
    "Thailand",
    "Togo",
    "Turkey","United Kingdom","United States","Uzbekistan","Vietnam","Malaysia",
  # Borrowed from Jed (and Phai) Thanks, needed the list.
'''

corrected_CountryNames = {
    "Angola",
    "Argentina",
    "Australia",
    "Austria",
    "Bangladesh",
    "Bolivia",
    "Brazil",
    "Cote d'Ivoire",
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
}


# Note, for dataset, I found something called 'SequenceMatcher' from difflib. ratio() method for comparison rate of categories, could be helpful

def get_most_similar(word,wordlist):
     top_similarity = 0.0
     most_similar_word = word
     for candidate in wordlist:
         similarity = SequenceMatcher(None,word,candidate).ratio()
         if similarity > top_similarity:
             top_similarity = similarity
             most_similar_word = candidate
     return most_similar_word

# Now apply to 'country' column

df['country'].apply(lambda x: get_most_similar(x, corrected_CountryNames)) # Maybe apply to someone else's cases?



'''
input_country_list=list(df['Country'])
input_country_list=[element.upper() for element in input_country_list];
def country_name_check():
pycntrylst = list(countries)
alpha_2 = []
alpha_3 = []
name = []
common_name = []
official_name = []
invalid_countrynames =[]
'''
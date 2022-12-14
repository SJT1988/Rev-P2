from time import strptime
import pyspark
from re import search
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import datetime as dt
import os
import countryNames_test # contribution from Kynghoon
from difflib import SequenceMatcher 

spark = SparkSession.builder \
    .master ("local") \
    .appName("myRDD") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext

_filepath = 'file:/home/strumunix/Rev-P2/phase_2/'
#_filepath = 'file:/home/jed/'
Out_Put_Path_for_Leading_in_qty = _filepath + "qty_out"
Out_Put_Path_for_Leading_in_value = _filepath +"value_out"
InPut_CSV_File_Path = _filepath +"p2_Team1_Data.csv"

#################################################################################################################################
#
#
#                                   CODE IMPLEMENTAION FROM KYUNGHOON                       
#
#
#################################################################################################################################
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
def Transform_from_incomming_RDD(RDDin):

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

    RDDin_1 = RDDin.rdd.map(lambda x :  (x['order_id'],x['customer_id'],x['customer_name'],x['product_id'], \
        x['product_name'],x['product_category'],x['payment_type'], x['qty'], x['price'],x['datetime'], \
        get_most_similar(x['country'],corrected_CountryNames),x['city'] ,x['e_commerce_website_name'], \
        x['payment_tnx_id'], x['payment_tnx_success'],x['failure_reason']))

    return RDDin_1
#################################################################################################################################
#################################################################################################################################
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
        #print (strdt)
    
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

    
def Check_spellings(myin):
    ListCountry = ['Angola','Argentina','Australia','Austria','Bangladesh','Bolivia','Brazil',"C??te d'Ivoire","C????te d'Ivoire",'Canada',\
    'Chdna','Chile','China','Colombia','Congo (Kinshasa)','Egypt','France','Greece','India','Indonesia','Iran','Iraq',\
    'Japan','Jhpan','Kazakhstan','Kenya','Kuwait','Malaysia','Mali','Mexico','Mongolia','Morocco','Nigeria','ohilippines',\
    'Pakistan','Peru','Philippines','Poland','Rassia','Russia','Saudi Arabia','Senegal','South Korea','Sudan','Tanzania',\
    'Thailand','Togo','Turkey','United Kingdom','United States','Unixed States','Uzbekistan','Vietnam','zalaysia','@']


    ListCity = ['Chicago','Seoul','Los Angeles','Chongqing','New York','Tangshan','Tehran','Rio de Janeiro','Paris',\
    'Manhattan','Bangkok','Boston','Tokyo','Shanghai','Kinshasa','Shaoyang','Mumbai','Seattle','Shenyang','Yuncheng','Philadelphia',\
    'Chennai','Ningde','Delhi','Ho Chi Minh City','Jiangguanchi','Mexico City','Beijing','Cairo','Jacksonville','Jakarta','Renqiu',\
    'Suzhou','Salt Lake City','??saka','????saka','Houston','Sacramento','Manila','Pingdu','Charlotte','Zhengzhou','Bangalore','Chengdu',\
    'Xinyang','Hermosillo','Dallas','Portland','Karachi','Baoding','Kolk??ta','Kolk????ta','Buenos Aires','Guangzhou','Rock Hill','Guangyuan',\
    'Almaty','Zhumadian','S??o Paulo','S????o Paulo','Handan','Lagos','Jeddah','Cangzhou','Nagoya','Heze','Shenzhen','Hanoi','Dhaka','Yokohama',\
    'Bel????m','San Francisco','Nanyang','Foshan','Miami','Melbourne','Moscow','Minneapolis','Istanbul','Weifang','Raipur','Atlanta',\
    'Lima','Zigong','Nur-Sultan','Mashhad','Shangrao','Shangqiu','Kawasaki','Alexandria','Juiz de Fora','Zunyi','Linyi','Xianyang',\
    'Far????d????b????d','Indore','Athens','Zhuzhou','Washington','San Jose','Zhanjiang','London','Saint Petersburg','Guadalajara',\
    'Qingdao','Anshan','Allah????b????d','Nanchong','Hyder????b????d','Chaoyang','Ganzhou','Richmond','Phoenix','Quanzhou','Lubuklinggau',\
    'Yantai','Kuwait City','Bengbu','Changping','Surabaya','Jinan','Xingtai','Kuala Lumpur','Brooklyn','Ji????????an','Hefei','San Diego',\
    'Shijiazhuang','Guw????h????ti','Zhoukou','Nada','Bijie','Orlando','Laredo','Detroit','Chuzhou','Yiyang','Lahore','Mexico Ccty','Mianyang',\
    'Tgmpa','Suihua','Huanggang','Busan','Bronx','Lucknow','Changsha','Hengyang','Yulin','Changzhou','Santiago','Fuzhou','Ulaanbaatar',\
    'Las Vegas','Tofyo','Baltimore','Chenzhou','Louisville','Khartoum','Xiangtan','Murbai','Giza','Nairobi','Tai????????an','Jiaozuo',\
    'Yingkou','Hanzhong','St. Louis','Ahmedabad','Taizhou','Weinan','Harrisburg','Xiamen','Xuanzhou','Changde','Montr????al','Austin',\
    'Igua????u','Indianapolis','Daegu','Laiwu','Jieyang','Naberezhnyye Chelny','Casablanca','Dazhou','Santa Cruz','Depok','Nanjing',\
    'Qujing','Shantou','Fuyang','Dar es Salaam','Jiamusi','Suqian','Wenzhou','Lu????????an','Kunming','Shuyangzha','Xinzhou','Baghdad',\
    'Nanchang','Taiyuan','Tongshan','Rosario','Luoyang','Sanzhou','Abidjan','Tianjin','Queens','Pittsburgh','Guigang','Adelaide',\
    'Pudong','Hickory','Leshan','Zhenjiang','Chengde','Tampa','Vadodara','Kansas City','Vienna','C??????n Th????','Bamako','Putian',\
    'Huaihua','Omaha','Rizhao','Xining','Bogot????','Dalian','Palm Bay','Cleveland','Changchun','Huizhou','Dongguan','Guankou','Shanwei',\
    'Mandya','Xinpu','Yichun','Rawalpindi','Xiangyang','Jining','Dakar','Pune','Mission Viejo','Riyadh','Warsaw','@']


    ListCountry_Correct = ['Angola','Argentina','Australia','Austria','Bangladesh','Bolivia','Brazil','Cote d\'Ivoire','Cote d\'Ivoire',\
    'Canada','China','Chile','China','Colombia','Congo (Kinshasa)','Egypt','France','Greece','India','Indonesia',\
    'Iran','Iraq','Japan','Japan','Kazakhstan','Kenya','Kuwait','Malaysia','Mali','Mexico','Mongolia','Morocco','Nigeria',\
    'Philippines','Pakistan','Peru','Philippines','Poland','Russia','Russia','Saudi Arabia','Senegal','South Korea','Sudan',\
    'Tanzania','Thailand','Togo','Turkey','United Kingdom','United States','United States','Uzbekistan','Vietnam','Malaysia','@']

    ListCity_Correct = ['Chicago','Seoul','Los Angeles','Chongqing','New York','Tangshan','Tehran','Rio de Janeiro', \
    'Paris','Manhattan','Bangkok','Boston','Tokyo','Shanghai','Kinshasa','Shaoyang','Mumbai','Seattle','Shenyang', \
    'Yuncheng', 'Philadelphia','Chennai','Ningde','Delhi','Ho Chi Minh City','Jiangguanchizhen','Mexico City','Beijing',\
    'Cairo','Jacksonville','Jakarta','Renqiu','Suzhou','Salt Lake City','Osaka','Osaka','Houston','Sacramento','Manila','Pingdu',\
    'Charlotte','Zhengzhou','Bangalore','Chengdu','Xinyang','Hermosillo','Dallas','Portland','Karachi','Baoding','Kolkata','Kolkata',\
    'Buenos Aires','Guangzhou','Rock Hill','Guangyuan','Almaty','Zhumadian','S??o Paulo','S??o Paulo','Handan','Lagos','Jeddah',\
    'Changzhou','Nagoya','Heze','Shenzhen','Hanoi','Dhaka','Yokohama','Belo','San Francisco','Nanyang','Foshan','Miami',\
    'Melbourne','Moscow','Minneapolis','Istanbul','Weifang','Raipur','Atlanta','Lima','Zigong','Nur-Sultan','Mashhad','Shangrao',\
    'Shangqiu','Kawasaki','Alexandria','Juiz de Fora','Zunyi','Linyi','Xianyang','Faridabad','Indore','Athens','Zhuzhou',\
    'Washington','San Jose','Zhanjiang','London','Saint Petersburg','Guadalajara','Qingdao','Anshan','Allahabad','Nanchong',\
    'Hyderabad','Chaoyang','Ganzhou','Richmond','Phoenix','Quanzhou','Lubuklinggau','Yantai','Kuwait City','Bengbu','Changping', \
    'Surabaya','Jinan','Xingtai','Kuala Lumpur','Brooklyn','Ji\'An','Hefei','San Diego','Shijiazhuang','Guwahati','Zhoukou',\
    'Nada','Bijie','Orlando','Laredo','Detroit','Chuzhou','Yiyang','Lahore','Mexico City','Mianyang','Tampa','Suihua',\
    'Huanggang','Busan','Bronx','Lucknow','Changsha','Hengyang','Yulin','Changzhou','Santiago','Fuzhou','Ulaanbaatar','Las Vegas',\
    'Tokyo','Baltimore','Chenzhou','Louisville','Khartoum','Xiangtan','Mumbai','Giza','Nairobi','Yichun','Jiaozuo','Yingkou','Hanzhong',\
    'St. Louis','Ahmedabad','Taizhou','Weinan','Harrisburg','Xiamen','Quanzhou','Changde','Montreal','Austin','Foz do Igua??u',\
    'Indianapolis','Daegu','Laiwu','Jieyang','Naberezhnyye Chelny','Casablanca','Dazhou','Santa Cruz','Depok','Nanjing',\
    'Qujing','Shantou','Fuyang','Dar es Salaam','Jiamusi','Suqian','Wenzhou','Lu\'An','Kunming','Shuyangzha','Xinzhou',\
    'Baghdad','Nanchang','Taiyuan','Tongshan','Rosario','Luoyang','Sanzhou','Abidjan','Tianjin','Queens','Pittsburgh','Guigang',\
    'Adelaide','Pudong','Hickory','Leshan','Zhenjiang','Chengde','Tampa','Vadodara','Kansas City','Vienna','C??ntT\'y','Bamako',\
    'Putian','Huaihua','Omaha','Rizhao','Xining','Bogota','Dalian','Palm Bay','Cleveland','Changchun','Huizhou','Dongguan',\
    'Guankou','Shanwei','Mandya','Xinpu','Yichun','Rawalpindi','Xiangyang','Jining','Dakar','Pune','Mission Viejo','Riyadh',\
    'Warsaw','@']

    Country = str(myin)
    #print (Country)
    #print (City)
    i = ListCountry.index(Country)
    return ListCountry_Correct[i]

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

#################################################################################################################################
#
#                                   USING KYUNGHOON'S IMPLEMENTAION
#
#################################################################################################################################

        
    rddx_7 = Transform_from_incomming_RDD(rddx_6)        

    rddx_8 = rddx_7.toDF(["order_id","customer_id","customer_name","product_id","product_name","product_category", \
        "payment_type","qty","price","datetime","country","city","e_commerce_website_name","payment_tnx_id", \
        "payment_tnx_success","failure_reason"])

#                                   THANK'S KYUNGHOON        
#################################################################################################################################
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
    rddxb2.createOrReplaceTempView("mydata")
    return rddxb2 
    #rddxb2.show()

def WriteCleanedData(CSV_FilePath,OutPut_FilePath):
    bb = Get_Cleaned_Data(CSV_FilePath)#'file:/home/jed/p2_Team1_Data.csv'
    bb.write.csv(OutPut_FilePath) #'file:/home/jed/Cleaned'
    print ("File Printed")

def Results_Q1(MyRDD):
    #################################################################################################################
    #
    #  Query data for the first question:
    #  What is the top selling category of items? Per country?
    #  There are two possible avenues 
    #  1. leadingLeading in terms of quantity sold,
    #  2. leading in terms of value sold
    #
    #
    #################################################################################################################

    #################################################################################################################
    #
    #                       Leading interms of Quanity Sold
    #
    #################################################################################################################

    df_Q1_2 = spark.sql("select sum(qty) as mycount, product_category, country from mydata group by \
    product_category,country order by country asc, mycount desc ")
    df_Q1_2.show(10) # the show statement diplays the results. within the brackets you can define the number of records
    # you want to see
    #################################################################################################################
    #
    #  saving the filtered results
    #
    #################################################################################################################

    df_Q1_2.createOrReplaceTempView("P1")

    '''
    ##################################################################################################################
    LEARNING

    spark sql is slightly different to mysql
    in my experince 
    In my experience, you cannot execute the same complex mysql queries in pyspark for example for Q1 in mysql you can
    run the following 
    command for a data table with the headings [???mycoint???, ???product_category???, ???country???]

    select max(mycount), product_category, country from t1 group by country order by country; 

    if you run the above mysql query in pyspark.sql it will warn you that product_category is not an aggregate function 
    or is not within the group by clause.If you include product_category into the group by clause you will have the same 
    data frame as seen above

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

    The benefit of providing column aliases is important to prevent column ambiguity in joining columns and later on 
    selecting the right columns from the joined table 
    ##################################################################################################################
    '''
    df_Q1_3_1 = spark.sql("select max(mycount) as mycount_n, country as country_n from P1 group by country order by country")
    #df_Q1_3_1_Value = spark.sql("select max(mycount) as mycount_n, country as country_n from P2 group by country order by country")
    df_Q1_3_1.show(10) # to display results
    #df_Q1_3_1.write.csv(_filepath+'out.csv.fullcategory')
    #df_Q1_3_1_Value.show(10)
    #df_Q1_3_1.write.csv('/home/jed/out.csv.fullcategory')

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

    if os.path.exists(Out_Put_Path_for_Leading_in_qty) == True:
        print ("Records not saved as the directory exists. Please delete it and re-run the code")
    else:
        df_Q1_3_3.write.csv(Out_Put_Path_for_Leading_in_qty)

    #df_Q1_3_3.write.csv(_filepath+'out.csv')
    #################################################################################################################
    #
    #                       Leading interms of Value Sold
    #
    #################################################################################################################   
    df_Q1_2_Value = MyRDD.rdd.map(lambda x : (x['product_category'],round(float(x['qty'])*float(x['price']),2), \
    x['country']))
    df_Q1_2_Value_x = df_Q1_2_Value.toDF(['product_category','value','country'])
    
    print ("Value")
    df_Q1_2_Value_x.show(10)

    df_Q1_2_Value_x.createOrReplaceTempView("P2")
    df_Q1_2_Value_x = spark.sql("select cast(value as float), country, product_Category from P2 order by country asc, value desc")
    df_Q1_2_Value_x.createOrReplaceTempView("P2")
  
    df_Q1_2_Value_x.show()
    df_Q1_2_Value_x_1 = spark.sql("select sum(value) as myvalue, product_category, country from P2 group by \
    product_category,country order by country asc, myvalue desc ")
    df_Q1_2_Value_x_1.createOrReplaceTempView("P2")

    df_Q1_2_Value_x_2 = spark.sql('select max(myvalue) as value_n, country as country_n from P2 group by country order by country')

    df_Q1_2_Value_x_3 = df_Q1_2_Value_x_2.join(df_Q1_2_Value_x_1,
    [df_Q1_2_Value_x_1.myvalue == df_Q1_2_Value_x_2.value_n, df_Q1_2_Value_x_1.country == df_Q1_2_Value_x_2.country_n],"inner")

    df_Q1_2_Value_x_4 = df_Q1_2_Value_x_3.select([df_Q1_2_Value_x_3.value_n, df_Q1_2_Value_x_3.product_category, 
    df_Q1_2_Value_x_3.country]).orderBy(df_Q1_2_Value_x_3.country)
    
    print ("Display")
    df_Q1_2_Value_x_4.show()

    if os.path.exists(Out_Put_Path_for_Leading_in_value) == True:
        print ("The File path exists. Records not saved. Please delete the folder and re-run the code")
    else:
        df_Q1_2_Value_x_4.write.csv(Out_Put_Path_for_Leading_in_value) 
        print ("CSV Printed")
#                               End of Q1 for P2    
#################################################################################################################

    
def main():
    #WriteCleanedData('file:/home/jed/p2_Team1_Data.csv','file:/home/jed/Cleaned')
    Results_Q1(Get_Cleaned_Data(InPut_CSV_File_Path))
    WriteCleanedData(_filepath+'p2_Team1_Data.csv',_filepath+'Cleaned')
    # in console type spark-submit pythonfilename.py
if __name__ == '__main__'  :
    main() 
print('SUCCESS')
spark.stop()

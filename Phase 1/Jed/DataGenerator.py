from itertools import count
from random import randint, shuffle, seed
from tokenize import Name
from faker.providers.person.en import Provider
import json as js
import datetime as dt

List_length = 15000
order_id = []
customer_id = []
customer_name = []
product_id = []
product_name = []
product_category_main = []
product_category_sub = []
product_category_sub_sub = []
payment_type = []
qty = []
price = []
datetiem = []
country = []
city = []
e_commerce_website = []
Payment_tnx_id = []
payment_tnx_success = []
failure_reason = []

def Random_Name_Generator():
    first_names = list(set(Provider.first_names))
    Lname = list (set(Provider.last_names))
    seed (1000)
    shuffle(first_names)
    shuffle(Lname)
    L = []
    for i in range (0,List_length):
        j = randint(0,6823)
        k = randint(0,472)
        if len(L) ==0 :
            customer_name.append(first_names[j] + " " + Lname[k])
        else:
            stt = str(first_names[j]) + " " + str(Lname[k])
            if not ( stt) in L:
                customer_name.append(first_names[j] + " " + Lname[k])

       
def Random_Order_Id_Generator():
    while len(order_id)<List_length:
        J = randint(0,999999)
        if order_id.count(J)==0:
            order_id.append(J)

def Random_customer_id_Generator():
    while len(customer_id)<List_length:
        J = randint(0,999999)
        if customer_id.count(J)==0:
            customer_id.append(J)

def Random_product_details_generator():
    Product_info = open("sample_product_product_categories.json").read()
    Product_info = js.loads(Product_info)
    for i in range (List_length):
        rd = randint(0,len(Product_info)-1)
        d = Product_info[rd]
        product_name.append(d['name'])
        product_id.append(d['sku'])
        st = d['breadcrumbs'].replace(',','-').split('~')
        product_category_main.append (st[0]) 
        product_category_sub.append(st[1])
        product_category_sub_sub.append(st[2])
        price.append(d['price'])
        qty.append(randint(0,10))

def Random_payment_generator():
    Payment_Types = [
        ('Card',['CVV','Gateway error','Declined by merchant', 'Insufficient funds']),
        ('Wallet',['Insifficient funds','Authentication error', 'Gateway error']),
        ('Electronic',['Authentication error', 'Gateway error'])
        ]

    for i in range (List_length):
        rd = randint(0,len(Payment_Types)-1)
        payment_type.append(tuple(Payment_Types[rd])[0])
        # print (Payment_Types[rd])
        # print (tuple(Payment_Types[rd])[0])
        rdd = randint(0,1)
        if rdd ==1:
            payment_tnx_success.append("Y")
            failure_reason.append("")
        else:
            payment_tnx_success.append("N")  
            rdx = randint(0,len(tuple(Payment_Types[rd])[1])-1)
            failure_reason.append(list(tuple(Payment_Types[rd])[1])[rdx])     
        
def Date_time_generator():
    for i in range (0, List_length):
        years = 2021
        months = randint(1,12)
        days = 0
        if months == 2:
            days = randint( 1,28)
        else:
            days = randint(1,30)

        Hours = randint(8,20)
        Mins = randint(0,59)
        seconds = randint(0,59)

        da = dt.datetime(year=years, month=months, day=days, hour=Hours,minute=Mins,second=seconds)   
        da = da.strftime('%Y-%m-%d %H:%M:%S') 

        datetiem.append(da)

def Country_city_generator():
    T = (["Slovalia", ["Kosice","Prešov","Trencin","Bratislava"]],\
        ["Hungary",["Debrecen","Gyor","Budapest"]],\
        ["Czech Republic", ["Brno","Czechia","Paragu"]],\
        ["Ireland",["Cork","Limerick","Galway","Dublin"]], \
        ["UK",["Accrington","Aberystwyth","Battle Hill","Chelmsford","Chathom","London"]])
    for i in range (0,List_length):
        rd = randint(0,len(T)-1)
        country.append(T[rd][0])
        rdx = randint(0,len(T[rd][1])-1)
        city.append(T[rd][1][rdx])

def e_commerce_website_generator():
    for i in range(0,List_length):
        T = ["Tesco","Amazon","e-bay"]
        rd = randint(0,len(T)-1)
        e_commerce_website.append(T[rd])

def payment_transaction_id_generator():
    while len(Payment_tnx_id)<List_length:
        J = randint(0,999999)
        if Payment_tnx_id.count(J)==0:
            Payment_tnx_id.append(J)



def main():
    #Random_Name_Generator()
    #Random_Order_Id_Generator()
    #Random_customer_id_Generator()
    #Random_product_details_generator()
    Random_payment_generator()
    Date_time_generator()
    #Country_city_generator()
    #e_commerce_website_generator()
    payment_transaction_id_generator()
    print(len(order_id),
        len(customer_id),
        len(customer_name),
        len(product_id),
        len(product_name),
        len(product_category_main),
        len(product_category_sub),
        len(product_category_sub_sub),
        len(payment_type),
        len(qty),
        len(price),
        len(datetiem),
        len(country), 
        len(city),
        len(e_commerce_website),
        "Tnx_id", len(Payment_tnx_id),
        len(payment_tnx_success),
        len(failure_reason))
    
    st = "payment_type,datetime,transaction_id,transaction_success,failure_reason\n"

    for i in range(0,List_length):
        st +=str(payment_type[i]) + "," + \
        str(datetiem[i]) + "," + \
        str(Payment_tnx_id[i]) + "," + \
        str(payment_tnx_success[i]) + "," +\
        str(failure_reason[i]) +'\n'
        #         
    
        #      st += str(order_id [i] ) + "," + \
        #     str(customer_id [i]) + "," + \
        #     str(customer_name[i]) + "," + \
        #     str(product_id[i]) + "," +  \
        #     str(product_name[i]) + "," + \
        #     str(product_category_main[i])+ "," + \
        #     str(product_category_sub[i]) + "," + \
        #     str(product_category_sub_sub[i]) + "," + \
        #     str(payment_type[i]) + "," + \
        #     str(qty[i]) + "," + \
        #     str(price[i]) + "," + \
        #     str(datetiem[i]) + "," + \
        #     str(country[i]) + "," + \
        #     str(city[i]) + "," + \
        #     str(e_commerce_website[i]) + "," + \
        #     str(Payment_tnx_id[i]) + "," + \
        #     str(payment_tnx_success[i]) + "," +\
        #     str(failure_reason[i]) +'\n'

    with open('data.csv','w') as w:
        w.write(st)

    print("Done")     
if __name__ == "__main__":
    main()

import random
from num2words import num2words


def combine_csvs():
    payment_file = open("payment_info.csv", "r", encoding="utf-8")
    customer_file = open("customer_info.csv", "r", encoding="utf-8")
    # item_file = open("item_info.csv", "r") #Kyunghoon's part, work in progress
    # qty_file = open("qty_info.csv", "r") #Jed's WIP
    n = 15000
    payment_list = []
    customer_list = []
    item_list = []
    qty_list = []
    for line in payment_file:
        tmp = line.split(",")
        payment_list.append(tmp)
    for line in customer_file:
        # For every file, we have to remove the newline character or else the data is split into new lines when generated.
        tmp = line.split(",")
        rec1 = tmp[0]
        rec2 = tmp[1]
        rec3 = tmp[2]
        rec4 = tmp[3]
        rec5 = rec4[:-2]
        tmp = [rec1, rec2, rec3, rec5]
        customer_list.append(tmp)
        # for line in item_file:
        #     tmp = line.split(",")
        #     item_list.append(tmp)
        #     tmp = line.split(",")
        #     rec1 = tmp[0]
        #     rec2 = tmp[1]
        #     rec3 = tmp[2]
        #     rec4 = rec3[:-2]
        #     tmp = [rec1, rec2, rec3, rec5]
        #     item_list.append(tmp)
        # for line in qty_list:
        #     tmp = line.split(",")
        #     qty_list.append(tmp)
        # with open("data.csv", "w") as f:
    payment_file.close()
    customer_file.close()
    data_file = open("data_csv", "w", encoding="utf-8")
    for i in range(1, n):
        rng = random.randint(0, 101)
        order_id = customer_list[i][0]
        customer_name = customer_list[i][1]
        # product_id = product_list[i][0]
        # product_name = product_list[i][1]
        # product_category = product_list[i][2]
        payment_type = payment_list[i][0]
        # qty = qty_list[i][0]
        # price = qty_list[i][1]
        country = customer_list[i][3]
        city = customer_list[i][2]
        # ecommerce_website_name = qty_list[i][2]
        payment_txn_id = payment_list[i][1]
        payment_txn_success = payment_list[i][2]
        failure_reason = payment_list[i][3]
        # 5% chance for line to be "roguefied"
        if rng < 6:
            order_id = num2words(order_id, to="ordinal")
        data_file.write(
            f"{order_id}, {customer_name}, {payment_type}, {country}, {city}, {payment_txn_id}, {payment_txn_success}, {failure_reason}"
        )

    data_file.close()


combine_csvs()

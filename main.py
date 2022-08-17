import random
from num2words import num2words

item_prices = {
    "Laptops": 450,
    "Hard Drives": 125,
    "Phones": 600,
    "Microwaves": 160,
    "Headphones": 130,
    "Electronic Locks": 85,
    "Drones": 50,
    "Computers": 700,
    "Tablets": 600,
    "TVs": 1200,
    "Remotes": 25,
    "Earbuds": 120,
    "Smart fridges": 349,
    "Mouse": 80,
    "Webcam": 120,
    "USB": 15,
    "Cables": 30,
    "Fridges": 270,
    "Monitors": 150,
    "Virtual Assistant": 179,
    "Jeans": 29,
    "Underwear": 9,
    "Shoes": 60,
    "Sandals": 30,
    "Formal Wear": 120,
    "Socks": 5,
    "Swimwear": 29,
    "Kahkis": 15,
    "Gowns": 29,
    "Dresses": 99,
    "T-shirts": 10,
    "Glasses": 59,
    "Sunglasses": 79,
    "Tracksuit": 49,
    "Suits": 120,
    "Jackets": 20,
    "Coats": 20,
    "Ties": 15,
    "Briefs": 5,
    "Boxers": 5,
    "Bow-ties": 15,
    "Scarves": 18,
    "Earrings": 80,
    "Belts": 42,
    "Wallet": 65,
    "Cap": 20,
    "Watch": 60,
    "Chairs": 60,
    "Couches": 300,
    "Recliners": 165,
    "Tables": 350,
    "Cabinets": 80,
    "Clocks": 100,
    "Bookcase": 150,
    "Dresser": 120,
    "Chest": 80,
    "Beds": 500,
    "Ladders": 23,
    "Ottoman": 50,
    "Stool": 25,
    "Cupboard": 90,
    "Cradle": 129,
    "Grandfather clock": 500,
    "Shelves": 30,
    "Notebooks": 1,
    "Pencils": 0.5,
    "Mechanical Pencils": 0.5,
    "Erasers": 1.5,
    "Chromebooks": 120,
    "Textbooks": 65,
    "Backpacks": 29,
    "Bags": 10,
    "Rulers": 2,
    "Crayons": 3,
    "Markers": 3,
    "Calculators": 60,
    "Uniforms": 20,
    "Sketchbooks": 3,
    "Notepads": 3,
    "Pens": 1.5,
    "Folders": 1,
    "Pencil Pouch": 5,
    "Glue Stick": 1,
    "Lunch Bags": 7,
    "Gym Bag": 12,
    "Sharpie pens": 2,
    "Lunchbox": 7,
    "Index cards": 2,
}


def combine_csvs():
    payment_file = open("payment_info.csv", "r", encoding="utf-8")
    customer_file = open("customer_info.csv", "r", encoding="utf-8")
    item_file = open("product_info.csv", "r")
    qty_file = open("qty_info.csv", "r")
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
        rec5 = rec4[:-1]
        tmp = [rec1, rec2, rec3, rec5]
        customer_list.append(tmp)
    for line in item_file:
        tmp = line.split(",")
        print(tmp)
        rec1 = tmp[0]
        rec2 = tmp[1]
        rec3 = tmp[2]
        rec4 = rec3[:-1]
        tmp = [rec1, rec2, rec4]
        item_list.append(tmp)
    for line in qty_file:
        tmp = line.split(",")
        rec1 = tmp[9].strip()
        rec2 = f"${tmp[10].strip()}"
        rec3 = tmp[11].strip()
        rec4 = tmp[14].strip()
        tmp = [rec1, rec2, rec3, rec4]
        qty_list.append(tmp)
    payment_file.close()
    customer_file.close()
    data_file = open("data.csv", "w", encoding="utf-8")
    for i in range(1, n + 1):
        rng = random.randint(0, 101)
        order_id = str(i)
        customer_id = customer_list[i][0]
        customer_name = customer_list[i][1]
        product_id = item_list[i][0]
        product_name = item_list[i][1]
        product_category = item_list[i][2]
        payment_type = payment_list[i][0]
        qty = qty_list[i][0]
        price_int = float(item_prices[f"{product_name}"]) * float(qty)
        price = str(f"${price_int}")
        datetime = qty_list[i][2]
        country = customer_list[i][3]
        city = customer_list[i][2]
        ecommerce_website_name = qty_list[i][3]
        payment_txn_id = payment_list[i][1].strip()
        payment_txn_success = payment_list[i][2].strip()
        failure_reason = payment_list[i][3].strip()
        # 5% chance for line to be "rogue field"
        if rng < 6:
            order_id = num2words(order_id, to="ordinal")
            #We need to get rid of extra commas for longer num2words:
            order_id = order_id.translate({ord(","): None})
        data_file.write(
            f"{order_id}, {customer_id}, {customer_name}, {product_id}, {product_name}, {product_category}, {payment_type}, {qty}, {price}, {datetime}, {ecommerce_website_name}, {city}, {country}, {payment_txn_id}, {payment_txn_success}, {failure_reason}\n"
        )

    data_file.close()


combine_csvs()

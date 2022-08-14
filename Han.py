# Kyunghoon Han
# Visual Studio Python 3

import csv
import random

class productInfo():
    # Generate at least a 1000 fake(?) products and product categories

    def product_id(num):
        lst = []
        for i in range(num):
            id = random.randint(10000, 100000)
            if id not in lst:
                lst.append(id)
            else:
                i -= 1
        return lst
    
    # Tech

    # Food

    # Clothes

    # Beverages

    # Appliances

    # Home Decor

    # Sports
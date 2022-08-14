# Kyunghoon Han
# Visual Studio Python 3

import csv, re
import random

class productInfo():
    # Generate at least a 1000 fake(?) products and product categories

    @staticmethod
    def fakeProducts(p):
        lst = []
        for i in range(p):
            product_id = random.randint(1, 15000)
            if product_id not in lst:
                lst.append(product_id)
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
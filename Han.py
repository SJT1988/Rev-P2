# Kyunghoon Han
# Visual Studio Python 3

import csv, re
import random

class productInfo():
    # Generate at least a 1000 fake(?) products and product categories

    @staticmethod
    def fakeProducts():
        num = 1000
        idNum = 0
        with open('product_info.csv', 'w', encoding='utf-8', newline = '') as file:
            
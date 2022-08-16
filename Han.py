# Kyunghoon Han
# Visual Studio Python 3

import csv
import random

# PRODUCT ID, PRODCUT NAME, PRODUCT CATEGORY


# Generate at least a 1000 fake(?) products and product categories
tech = ['Laptops', 'Hard Drives', 'Phones', 'Microwaves', 'Headphones', 'Electronic Locks', 'Drones', 'Computers', 'Tablets', 'TVs', 'Remotes', 'Earbuds',
                'Smart fridges', 'Mouse', 'Webcam', 'USB', 'Cables']

clothes = ['Jeans', 'Underwear', 'Shoes', 'Sandals', 'Formal Wear', 'Socks', 'Swimwear', 'Kahkis', 'Gowns', 'Dresses', 'T-shirts', 'Glasses', 'Sunglasses']

furniture = ['Chairs', 'Couches', 'Recliners', 'Tables', 'Cabinets', 'Clocks', 'Bookcase', 'Dresser', 'Chest', 'Beds']

school = ['Notebooks', 'Pencils', 'Mechanical Pencils', 'Erasers', 'Chromebooks', 'Textbooks', 'Backpacks', 'Bags', 'Rulers']

dict_type = {tech:tuple(['Laptops', 'Hard Drives', 'Phones', 'Microwaves', 'Headphones', 'Electronic Locks', 'Drones', 'Computers', 'Tablets', 'TVs', 'Remotes', 'Earbuds','Smart fridges', 'Mouse', 'Webcam', 'USB', 'Cables']), 
    clothes:tuple(['Jeans', 'Underwear', 'Shoes', 'Sandals', 'Formal Wear', 'Socks', 'Swimwear', 'Kahkis', 'Gowns', 'Dresses', 'T-shirts', 'Glasses', 'Sunglasses']), 
    furniture:tuple(['Chairs', 'Couches', 'Recliners', 'Tables', 'Cabinets', 'Clocks', 'Bookcase', 'Dresser', 'Chest', 'Beds']), 
    school:tuple(['Notebooks', 'Pencils', 'Mechanical Pencils', 'Erasers', 'Chromebooks', 'Textbooks', 'Backpacks', 'Bags', 'Rulers'])}

lastproduct_selection = random.choice(list(dict_type))
product_type = cat_selection[0]

def tech():
    n = 1500
    id = 0
    with open('product_info.csv', 'w', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['ID', 'Product', 'Category'])

        for i in range(n):
            id += 1
            pID = 'file{id:05}'
            writer.writerow([pID, random.choice(lastproduct_selection[1]), product_type])





'''
list_size = ['tiny','little, 'medium', 'big', 'honkin\'']
list_color = ['red','blue','green','orange']
dict_type = {kitchen: ['toaster','microwave'],
furniture: ['sofa','desk'],
electronics: ['television','stereo'], fruit: ['pineapple','bannana']}

last_selection = random.choice(list(dict_type))

item_name=random.choice(list_size) + random.choice(list_color) + random.choice(last_selection[1])

item_type = last_selection[0]
'''
# Kyunghoon Han
# Visual Studio Python 3

import csv
import random

# PRODUCT ID, PRODCUT NAME, PRODUCT CATEGORY

class productInfo():
# Generate at least a 1000 fake(?) products and product categories

#dict_type = {tech:tuple(['Laptops', 'Hard Drives', 'Phones', 'Microwaves', 'Headphones', 'Electronic Locks', 'Drones', 'Computers', 'Tablets', 'TVs', 'Remotes', 'Earbuds','Smart fridges', 'Mouse', 'Webcam', 'USB', 'Cables']), 
#    clothes:tuple(['Jeans', 'Underwear', 'Shoes', 'Sandals', 'Formal Wear', 'Socks', 'Swimwear', 'Kahkis', 'Gowns', 'Dresses', 'T-shirts', 'Glasses', 'Sunglasses']), 
#    furniture:tuple(['Chairs', 'Couches', 'Recliners', 'Tables', 'Cabinets', 'Clocks', 'Bookcase', 'Dresser', 'Chest', 'Beds']), 
#    school:tuple(['Notebooks', 'Pencils', 'Mechanical Pencils', 'Erasers', 'Chromebooks', 'Textbooks', 'Backpacks', 'Bags', 'Rulers'])}

#lastproduct_selection = random.choice(list(dict_type))
#product_type = lastproduct_selection[0]

    def main():
        tech = ['Laptops', 'Hard Drives', 'Phones', 'Microwaves', 'Headphones', 'Electronic Locks', 'Drones', 'Computers', 'Tablets', 'TVs', 'Remotes', 'Earbuds',
                'Smart fridges', 'Mouse', 'Webcam', 'USB', 'Cables', 'Fridges', 'Monitors', 'Microwaves', 'Virtual Assistant']

        clothes = ['Jeans', 'Underwear', 'Shoes', 'Sandals', 'Formal Wear', 'Socks', 'Swimwear', 'Kahkis', 'Gowns', 'Dresses', 'T-shirts', 'Glasses', 'Sunglasses', 'Tracksuit',
                    'Suits', 'Jackets', 'Coats', 'Ties', 'Briefs', 'Boxers', 'Bow-ties', 'Scarves', 'Earrings', 'Belts', 'Wallet', 'Cap', 'Watch']

        furniture = ['Chairs', 'Couches', 'Recliners', 'Tables', 'Cabinets', 'Clocks', 'Bookcase', 'Dresser', 'Chest', 'Beds', 'Ladders', 'Ottoman', 'Stool', 'Cupboard',
                    'Cradle', 'Grandfather clock', 'Shelves']

        school = ['Notebooks', 'Pencils', 'Mechanical Pencils', 'Erasers', 'Chromebooks', 'Textbooks', 'Backpacks', 'Bags', 'Rulers', 'Crayons', 'Markers', 'Calculators',
                'Uniforms', 'Sketchbooks', 'Notepads', 'Pens', 'Folders', 'Pencil Pouch', 'Glue Stick', 'Lunch Bags', 'Gym Bag', 'Sharpie pens', 'Lunchbox', 'Index cards'] 

        n = 300
        id = 0
        with open('product_info.csv', 'w', encoding='utf-8', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['ID', 'Product', 'Category'])

            for i in range(n):
                id += 1
                writer.writerow([id, random.choice(tech), "Technology"])

            for i in range(n):
                id += 1
                writer.writerow([id, random.choice(clothes), "Clothes"])

            for i in range(n):
                id += 1
                writer.writerow([id, random.choice(furniture), "Furniture"])

            for i in range(n):
                id += 1
                writer.writerow([id, random.choice(school), "School Items"])



if __name__ == '__main__':
    productInfo.main()
    



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
# Kyunghoon Han
# Visual Studio Python 3

import csv
import random

# PRODUCT ID, PRODCUT NAME, PRODUCT CATEGORY


class productInfo:
    # Generate at least a 1000 fake(?) products and product categories

    # dict_type = {tech:tuple(['Laptops', 'Hard Drives', 'Phones', 'Microwaves', 'Headphones', 'Electronic Locks', 'Drones', 'Computers', 'Tablets', 'TVs', 'Remotes', 'Earbuds','Smart fridges', 'Mouse', 'Webcam', 'USB', 'Cables']),
    #    clothes:tuple(['Jeans', 'Underwear', 'Shoes', 'Sandals', 'Formal Wear', 'Socks', 'Swimwear', 'Kahkis', 'Gowns', 'Dresses', 'T-shirts', 'Glasses', 'Sunglasses']),
    #    furniture:tuple(['Chairs', 'Couches', 'Recliners', 'Tables', 'Cabinets', 'Clocks', 'Bookcase', 'Dresser', 'Chest', 'Beds']),
    #    school:tuple(['Notebooks', 'Pencils', 'Mechanical Pencils', 'Erasers', 'Chromebooks', 'Textbooks', 'Backpacks', 'Bags', 'Rulers'])}

    # lastproduct_selection = random.choice(list(dict_type))
    # product_type = lastproduct_selection[0]

    def main():
        tech = [
            "Laptops",
            "Hard Drives",
            "Phones",
            "Microwaves",
            "Headphones",
            "Electronic Locks",
            "Drones",
            "Computers",
            "Tablets",
            "TVs",
            "Remotes",
            "Earbuds",
            "Smart fridges",
            "Mouse",
            "Webcam",
            "USB",
            "Cables",
            "Fridges",
            "Monitors",
            "Microwaves",
            "Virtual Assistant",
        ]
        tech_id = []
        for i in range(len(tech)):
            basic_id = 100
            id = basic_id + i
            tech_id.append(id)

        clothes = [
            "Jeans",
            "Underwear",
            "Shoes",
            "Sandals",
            "Formal Wear",
            "Socks",
            "Swimwear",
            "Kahkis",
            "Gowns",
            "Dresses",
            "T-shirts",
            "Glasses",
            "Sunglasses",
            "Tracksuit",
            "Suits",
            "Jackets",
            "Coats",
            "Ties",
            "Briefs",
            "Boxers",
            "Bow-ties",
            "Scarves",
            "Earrings",
            "Belts",
            "Wallet",
            "Cap",
            "Watch",
        ]
        clothes_id = []
        for i in range(len(clothes)):
            basic_id = 200
            id = basic_id + i
            clothes_id.append(id)

        furniture = [
            "Chairs",
            "Couches",
            "Recliners",
            "Tables",
            "Cabinets",
            "Clocks",
            "Bookcase",
            "Dresser",
            "Chest",
            "Beds",
            "Ladders",
            "Ottoman",
            "Stool",
            "Cupboard",
            "Cradle",
            "Grandfather clock",
            "Shelves",
        ]
        furniture_id = []
        for i in range(len(furniture)):
            basic_id = 300
            id = basic_id + i
            furniture_id.append(id)

        school = [
            "Notebooks",
            "Pencils",
            "Mechanical Pencils",
            "Erasers",
            "Chromebooks",
            "Textbooks",
            "Backpacks",
            "Bags",
            "Rulers",
            "Crayons",
            "Markers",
            "Calculators",
            "Uniforms",
            "Sketchbooks",
            "Notepads",
            "Pens",
            "Folders",
            "Pencil Pouch",
            "Glue Stick",
            "Lunch Bags",
            "Gym Bag",
            "Sharpie pens",
            "Lunchbox",
            "Index cards",
        ]
        school_id = []
        for i in range(len(school)):
            basic_id = 400
            id = basic_id + i
            school_id.append(id)

        n = 15000
        with open("product_info.csv", "w", encoding="utf-8", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["ID", "Product", "Category"])

            for i in range(n):
                rng = random.randint(1, 4)
                if rng == 1:
                    choice = random.choice(tech)
                    index = tech.index(choice)
                    id = tech_id[index]
                    file.write(f"{id},{choice},Technology\n")
                if rng == 2:
                    choice = random.choice(clothes)
                    index = clothes.index(choice)
                    id = clothes_id[index]
                    file.write(f"{id},{choice},Clothes\n")
                if rng == 3:
                    choice = random.choice(furniture)
                    index = furniture.index(choice)
                    id = furniture_id[index]
                    file.write(f"{id},{choice},Furniture\n")
                if rng == 4:
                    choice = random.choice(school)
                    index = school.index(choice)
                    id = school_id[index]
                    file.write(f"{id},{choice},School Items\n")

            # for i in range(n):
            #     id += 1
            #     writer.writerow([id, random.choice(tech), "Technology"])

            # for i in range(n):
            #     id += 1
            #     writer.writerow([id, random.choice(clothes), "Clothes"])

            # for i in range(n):
            #     id += 1
            #     writer.writerow([id, random.choice(furniture), "Furniture"])

            # for i in range(n):
            #     id += 1
            #     writer.writerow([id, random.choice(school), "School Items"])


if __name__ == "__main__":
    productInfo.main()


"""
list_size = ['tiny','little, 'medium', 'big', 'honkin\'']
list_color = ['red','blue','green','orange']
dict_type = {kitchen: ['toaster','microwave'],
furniture: ['sofa','desk'],
electronics: ['television','stereo'], fruit: ['pineapple','bannana']}

last_selection = random.choice(list(dict_type))

item_name=random.choice(list_size) + random.choice(list_color) + random.choice(last_selection[1])

item_type = last_selection[0]
"""

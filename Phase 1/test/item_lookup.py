# test by Spencer Trumbore
import csv, random

# adjectives have multipliers.
adjective_list = [('stinky',0.25),('tiny',0.33),('small',0.67),('medium',1.00),('large',1.25), ('extra-large',1.50), ('cheap',0.25), ('modest',0.50), ('expensive',2.50), ('gorgeous',2.00)]
# colors have no multipliers.
color_list = ['red', 'orange', 'yellow','charteuse', 'green', 'cyan', 'blue', 'violet', 'magenta', 'rose']
# items have specific base-costs.
# appliances, electronics, furniture, clothes, fruit
item_list = [
    [('microwave',300.00),('toaster',20.00),('washing machine',1000.00),('grill',100.00),('refrigerator',1500.00)],
    [('television',500.00),('lamp', 50.00), ('alarm clock',15.00), ('smartphone',400.00), ('computer',1200.00)],
    [('couch',2000.00),('chair', 200.00),('bed',2000.00),('dresser',1000.00),('desk',1000.00)],
    [('pajamas',30.00),('swimsuit',25.00),('tuxedo',1000.00),('fedora',40.00),('trousers', 40.00)],
    [('bananna',0.35),('pineapple',1.50),('watermellon',6.00),('tomato',1.00),('lemon',0.75)]
    ]

# generate 2500 unique items
def generate_item_lookup_table():
    header_list = ['product_id', 'product_category', 'product_name', 'unit_price']
    
    temp_list = []
    accumulator = 1
    for cat in range(len(item_list)):
        for i in range(len(item_list[cat])):
            for j in color_list:
                for k in adjective_list:
                    item = f"{k[0]} {j} {item_list[cat][i][0]}"
                    unit_price = item_list[cat][i][1]*k[1]
                    p_id = 'p_' + f'{accumulator:05}'
                    category = None
                    if cat == 0:
                        category = 'appliances'
                    if cat == 1:
                        category = 'electronics'
                    if cat == 2:
                        category = 'furniture'
                    if cat == 3:
                        category = 'clothes'
                    if cat == 4:
                        category = 'fruit'
                    temp_list.append([p_id, category, item, unit_price])
                    accumulator+=1   
    random.shuffle(temp_list) # this will assist random selection later

    with open('test/item_lookup.csv','w') as f:
        writer = csv.writer(f,lineterminator='\n')
        writer.writerow(header_list)
        for row in temp_list:
            writer.writerow([row[0], row[1], row[2], f'${row[3]:.2f}'])
    
    return

generate_item_lookup_table()
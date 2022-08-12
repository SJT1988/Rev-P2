# AUTHOR: Spencer Trumbore
# Generates csv file containing values for
# 4 fields: customer_id, customer_name,
# city, country

import csv, re
from faker import Faker

#================================================================================
#================================================================================
class CustomerInfo():
    #============================================================================
    #============================================================================
    # generate 1000 fake names and cities for 15 countries using Faker
    @staticmethod
    def fakeItToMakeIt():
        n=1000
        id = 0
        with open('customer_info.csv', 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Id','Name', 'City', 'Country'])

            #US
            faker = Faker('en_US')
            for i in range(n):
                id+=1
                writer.writerow([id,faker.first_name() + ' ' + faker.last_name(), faker.city(), 'United States'])
            del(faker)

            #Britain
            faker = Faker(['en_GB'])
            for i in range(n):
                id+=1
                writer.writerow([id,faker.first_name() + ' ' + faker.last_name(), faker.city(), 'Great Brittain'])
            del(faker)

            #Australia
            faker = Faker(['en_AU'])
            for i in range(n):
                id+=1
                writer.writerow([id,faker.first_name() + ' ' + faker.last_name(), faker.city(), 'Australia'])
            del(faker)

            #Ireland
            faker = Faker('en_IE')
            for i in range(n):
                id+=1
                writer.writerow([id,faker.first_name() + ' ' + faker.last_name(), faker.city(), 'Ireland'])
            del(faker)

            #France
            faker = Faker(['fr_FR'])
            for i in range(n):
                id+=1
                writer.writerow([id,faker.first_name() + ' ' + faker.last_name(), faker.city(), 'France'])
            del(faker)

            #Germany
            faker = Faker(['de_DE'])
            for i in range(n):
                id+=1
                writer.writerow([id,faker.first_name() + ' ' + faker.last_name(), faker.city(), 'Germany'])
            del(faker)

            #Mexico
            faker = Faker('es_MX')
            for i in range(n):
                id+=1
                writer.writerow([id,faker.first_name() + ' ' + faker.last_name(), faker.city(), 'Mexico'])
            del(faker)

            #Brazil
            faker = Faker('pt_BR')
            for i in range(n):
                id+=1
                writer.writerow([id,faker.first_name() + ' ' + faker.last_name(), faker.city(), 'Brazil'])
            del(faker)

            #Ukraine
            faker = Faker(['uk_UA'])
            for i in range(n):
                id+=1
                writer.writerow([id,faker.first_name() + ' ' + faker.last_name(), faker.city(), 'Ukraine'])
            del(faker)

            #Russia
            faker = Faker(['ru_RU'])
            for i in range(n):
                id+=1
                writer.writerow([id,faker.first_name() + ' ' + faker.last_name(), faker.city(), 'Russia'])
            del(faker)

            #Iran
            faker = Faker(['fa_IR'])
            for i in range(n):
                id+=1
                writer.writerow([id,faker.first_name() + ' ' + faker.last_name(), faker.city(), 'Iran'])
            del(faker)

            #India
            faker = Faker('en_IN')
            for i in range(n):
                id+=1
                writer.writerow([id,faker.first_name() + ' ' + faker.last_name(), faker.city(), 'India'])
            del(faker)

            #China
            faker = Faker('zh_CN')
            for i in range(n):
                id+=1
                writer.writerow([id,faker.first_name() + ' ' + faker.last_name(), faker.city(), 'China'])
            del(faker)

            #Japan
            faker = Faker('ja_JP')
            for i in range(n):
                id+=1
                writer.writerow([id,faker.first_name() + ' ' + faker.last_name(), faker.city(), 'Japan'])
            del(faker)

            #Korea
            faker = Faker('ko_KR')
            for i in range(n):
                id+=1
                writer.writerow([id,faker.first_name() + ' ' + faker.last_name(), faker.city(), 'Korea'])
            del(faker)
        return

if __name__ == '__main__':
    CustomerInfo.fakeItToMakeIt()

'''    
    @staticmethod
    def processFakeNameGenerator():
        si_csv = 'fake_names\Arabic_Tunisia\FakeNameGenerator.com_67f4929e.csv'
        lst = []

        with open(si_csv, 'r', encoding = 'utf-8') as f:
            csv_reader = csv.reader(f)
            next(f)
            for line in f:
                line = line.strip().split(',')
                lst.append(line)

        for i in range(len(lst)):
            lst[i].pop(0)
            #lst[i].pop(-1)

        for x in lst:
            print(x)


    def getCustomerNames():
        
        si_csv = 'secret-identities1.csv'
        names = []
        cartoon_names = ['Tommy Pickles','Rick Sanchez', 'Morty Smith', 'Helga Pataki',
        'Chucky Finster', 'Homer Simpson', 'Peter Griffin', 'Sandy Cheeks', 'Velma Dinkley',
        'Eliza Thornberry', "April O'Neil", 'Tina Belcher', 'Lana Kane', 'Patrick Star',
        'Eric Cartman', 'Norville Rogers', 'Hank Hill', 'Daria Morgendorffer', 'Malala Yousafzai',
        'Azadeh Shahshahani']

        with open(si_csv, 'r',encoding='utf-8') as f:
            next(f)
            for line in f:
                line = line.strip()
                names.append(line)
        
        # process names
        for i in range(len(names)):
            names[i] = names[i].split()
            for j in range(len(names[i])):
                # pop nicknames and initials
                if re.match(r'^(“.+”)$|(.\.)$', names[i][j]):
                    names[i].pop(j)
                    break
            #finally, make sure only first and last are kept, for simplicity:
            names[i] = [names[i][0],names[i][-1]]
        
        # now we have 102 first-last name pairs. Append cartoon names:
        
        for n in names:
            print(n)
    pass
    '''
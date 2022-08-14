import random
import csv

# Generates n number of txn id where every txn id is unique
def payment_txn_id(n):
    lst = []
    i = 0
    while i < n:
        txn_id = random.randint(10000, 100000)
        if txn_id not in lst:
            lst.append(txn_id)
            i += 1
    return lst


# Rng for payment success to be approx 75%
def payment_txn_success():
    rng = random.randint(0, 101)
    if rng < 75:
        return True
    else:
        return False


def payment_type():
    rng = random.randint(0, 101)
    if rng >= 0 and rng <= 20:
        return "Card"
    elif rng >= 21 and rng <= 40:
        return "Electronic"
    elif rng >= 41 and rng <= 60:
        return "Internet Banking"
    elif rng >= 61 and rng <= 80:
        return "Gift card"
    else:
        return "Cash"


def payment_failure_reason(success, payment_type):
    if success == False:
        rng = random.randint(0, 100)
        if payment_type == "Cash" or payment_type == "Gift card":
            return "Insufficent funds"
        elif payment_type == "Electronic" or payment_type == "Internet Banking":
            return "Account locked"
        elif payment_type == "Card" and (rng >= 0 and rng <= 33):
            return "Invalid zip code"
        elif payment_type == "Card" and (rng >= 34 and rng <= 66):
            return "Invalid CVV"
        else:
            return "Invalid card number"
    else:
        return ""


def main():
    with open("payment_info.csv", "w") as f:
        n = 15000
        f.write("payment type, payment trans id, payment trans success, failure reason")
        lst = payment_txn_id(n)
        for i in range(n):
            temp_type = payment_type()
            if payment_txn_success() == True:
                temp_success = "Y"
                tmp = True
            else:
                temp_success = "N"
                tmp = False
            temp_reason = payment_failure_reason(tmp, temp_type)
            line = f"{temp_type}, {lst[i]}, {temp_success}, {temp_reason}\n"
            f.write(line)


if __name__ == "__main__":
    main()

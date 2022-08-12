import random


def payment_txn_id():
    txn_id = random.randint(10000, 100000)
    return txn_id


def payment_txn_success():
    rng = random.randint(0, 101)
    if rng < 75:
        return True
    else:
        return False


def payment_failure_reason(success, payment_type):
    if success == True:
        return ""
    else:
        rng = random.randint(0, 100)
        if (rng >= 0 and rng <= 20) or payment_type.lower() == "cash":
            return "Insufficent funds"
        elif rng >= 21 and rng <= 40:
            return "Account locked"
        elif rng >= 41 and rng <= 60:
            return "Invalid zip code"
        elif rng >= 61 and rng <= 80:
            return "Invalid CVV"
        else:
            return "Invalid card number"

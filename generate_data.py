import src.DatabaseManager as DBM
import src.DataGenerator as DG
import src.CurrencyConvertor as CC
import src.TransactionGenerator as TG
import src.utility as util

import random


# Initialize all DataGenerator classes
UserGen = DG.UserGenerator()
DeviceGen = DG.DeviceGenerator()
PaymentMethodGen = DG.PaymentMethodGenerator()
MerchantGen = DG.MerchantGenerator()

# Initialize DataBaseManage and CurrencyConvertor classes
DBManager = DBM.DatabaseManager()
CurrencyConvertor = CC.CurrencyConvertor()

# Get conv rates and initialize TransactionGenerator
conversion_rates = CurrencyConvertor.fetch_conversion_rates()
TransactionGen = TG.TransactionGenerator(conversion_rates=conversion_rates)

NR_OF_MERCHANTS = 50
NR_OF_USERS = 250
MIN_PATTERNS = 3
MAX_PATTERNS = 12

for m in range(NR_OF_MERCHANTS):
    print(f"Generating Merchant: {m}")
    merchant_data = MerchantGen.generate_merchant()
    DBManager.insert_merchant(merchant_data)

merchant_ids = DBManager.fetch_all_merchant_ids()

for u in range(NR_OF_USERS):
    print(f"Generating User: {u}")
    generated_timestamp = util.generate_random_past_timestamp()
    user = UserGen.generate_user(generated_timestamp)
    user_id = DBManager.insert_user(user)

    user_device = DeviceGen.generate_device(user_id, generated_timestamp)
    device_id = DBManager.insert_device(user_device)

    user_payment_method = PaymentMethodGen.generate_payment_method(user_id, generated_timestamp)
    DBManager.insert_payment_method(user_payment_method)

    for _ in range(random.randint(MIN_PATTERNS, MAX_PATTERNS)):
        # Generated_timestamp equal to user created at
        merchant_id = random.choice(merchant_ids)
        data = TransactionGen.generate_transaction_pattern(user_id, device_id, merchant_id, user_created_at=generated_timestamp)
        for transaction in data:
            DBManager.insert_transaction(transaction)

import csv
import os
from faker import Faker
from datetime import datetime
import random
import time

fake = Faker()
total_size_mb = 10
num_parts = 10
approx_rows_per_mb = 2000  # Assuming each row is around 0.5 KB
rows_per_file = approx_rows_per_mb

base_filename = 'bank_dataset'
directory = 'your_file_directory' #Replace with your exact file directory
if not os.path.exists(directory):
    os.makedirs(directory)

def generate_inconsistent_data():
    """Generates inconsistent data for testing ETL pipeline."""
    return [
        fake.random_int(min=1, max=1000000),  # TransactionID
        fake.bban(),  # AccountNumber
        fake.random_int(min=1000, max=9999),  # CustomerID
        fake.date_time_this_year().isoformat(),  # TransactionDate
        fake.random_element(elements=('deposit', 'withdrawal', 'transfer')),  # TransactionType
        fake.random_number(digits=5, fix_len=True) / 100 if random.choice([True, False]) else -fake.random_number(digits=5, fix_len=True) / 100,  # TransactionAmount (inconsistent: sometimes negative)
        fake.currency_code(),  # Currency
        fake.random_number(digits=7, fix_len=True) / 100 if random.choice([True, False]) else fake.random_number(digits=10, fix_len=True) / 100,  # AccountBalance (inconsistent: sometimes very large)
        fake.random_int(min=1, max=100),  # BranchCode
        fake.random_int(min=1, max=1000) if random.choice([True, False]) else '',  # MerchantCode (inconsistent: sometimes missing)
        fake.sentence(nb_words=3),  # Description
        fake.random_element(elements=('completed', 'pending')),  # TransactionStatus
        fake.random_int(min=0, max=1),  # FraudFlag
        fake.random_int(min=18, max=90),  # CustomerAge
        fake.random_element(elements=('M', 'F')),  # CustomerGender
        fake.random_int(min=20000, max=150000),  # CustomerIncome
        fake.city(),  # CustomerLocation
        fake.random_element(elements=('savings', 'checking')),  # AccountType
        fake.date_between(start_date='-10y', end_date='today').isoformat() if random.choice([True, False]) else 'Invalid Date',  # AccountOpeningDate (inconsistent: sometimes invalid date)
        fake.job()  # CustomerOccupation
    ]

# Create the dataset parts
for i in range(num_parts):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    unique_id = f"{i+1}"
    filename = f"{base_filename}_{timestamp}_{unique_id}.csv"
    filepath = os.path.join(directory, filename)

    with open(filepath, mode='w', newline='') as file:
        writer = csv.writer(file)
        # Write header
        writer.writerow([
            'TransactionID', 'AccountNumber', 'CustomerID', 'TransactionDate', 'TransactionType',
            'TransactionAmount', 'Currency', 'AccountBalance', 'BranchCode', 'MerchantCode',
            'Description', 'TransactionStatus', 'FraudFlag', 'CustomerAge', 'CustomerGender',
            'CustomerIncome', 'CustomerLocation', 'AccountType', 'AccountOpeningDate', 'CustomerOccupation'
        ])
        # Write rows
        for _ in range(rows_per_file):
            writer.writerow(generate_inconsistent_data())

    # Introduce a small delay to ensure different timestamps
    time.sleep(1)

print("Dataset generation complete.")

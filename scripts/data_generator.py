from datetime import datetime, timedelta
import pandas as pd
import random
import os
import sys

# -------- DATE INPUT --------
if len(sys.argv) > 1:
    run_date = sys.argv[1]
else:
    run_date = datetime.now().strftime("%Y-%m-%d")

# -------- PATHS --------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LANDING_PATH = os.path.join(BASE_DIR, "data", "landing")

os.makedirs(LANDING_PATH, exist_ok=True)

print(f"📅 Generating data for: {run_date}")

# -------------------------------
# CUSTOMERS (with dirty data)
# -------------------------------
customers = []
for i in range(1, 101):
    customers.append({
        "customer_id": f"C{i}" if random.random() > 0.02 else None,  # null id
        "first_name": f"Name{i}",
        "last_name": f"Surname{i}",
        "email": f"user{i}@mail.com" if random.random() > 0.1 else None,  # null email
        "phone": f"90000{i:05}",
        "city": random.choice(["Mumbai", "Delhi", "Hyderabad", None]),  # null city
        "country": "India",
        "updated_at": f"{run_date} {random.randint(10,23)}:{random.randint(10,59)}:00"
    })

# Duplicate some customers
customers += random.sample(customers, 5)

customers_df = pd.DataFrame(customers)

# -------------------------------
# ACCOUNTS (with dirty data)
# -------------------------------
accounts = []
for i in range(1, 101):
    accounts.append({
        "account_id": f"A{i}" if random.random() > 0.02 else None,
        "customer_id": f"C{i}" if random.random() > 0.05 else f"C{random.randint(200,300)}",  # invalid FK
        "account_type": random.choice(["SAVINGS", "CURRENT", "savings"]),  # case issue
        "balance": round(random.uniform(-5000, 50000), 2),  # negative balance
        "created_at": run_date
    })

# Add duplicates
accounts += random.sample(accounts, 5)

accounts_df = pd.DataFrame(accounts)

# -------------------------------
# TRANSACTIONS (with dirty data)
# -------------------------------
transactions = []
for i in range(1, 1001):
    cust_id = random.randint(1, 100)

    transactions.append({
        "transaction_id": f"T{i}" if random.random() > 0.01 else f"T{random.randint(1,50)}",  # duplicate ID
        "account_id": f"A{cust_id}",
        "customer_id": f"C{cust_id}",
        "amount": round(random.uniform(-100, 20000), 2),  # negative values
        "transaction_type": random.choice(["debit", "credit", "DEBIT", None]),  # case + null
        "merchant": random.choice(["Amazon", "Flipkart", "Swiggy", "Zomato", None]),
        "transaction_date": f"{run_date} {random.randint(0,23)}:{random.randint(0,59)}:00"
    })

# Add duplicate rows
transactions += random.sample(transactions, 20)

transactions_df = pd.DataFrame(transactions)

# -------------------------------
# WRITE FILES
# -------------------------------
customers_file = os.path.join(LANDING_PATH, f"customers_{run_date}.csv")
accounts_file = os.path.join(LANDING_PATH, f"accounts_{run_date}.csv")
transactions_file = os.path.join(LANDING_PATH, f"transactions_{run_date}.csv")

customers_df.to_csv(customers_file, index=False)
accounts_df.to_csv(accounts_file, index=False)
transactions_df.to_csv(transactions_file, index=False)

print("✅ Files generated with realistic dirty data:")
print(customers_file)
print(accounts_file)
print(transactions_file)
import os
import random
import pandas as pd
from datetime import datetime, timedelta

os.makedirs("data/input/realtime", exist_ok=True)

rows = []

statuses = ["completed", "cancelled", "pending", "returned", "INVALID"]
payment_methods = ["credit_card", "debit_card", "paypal", "cash", "INVALID"]
countries = ["Belgium", "France", "Germany", "", "belgium"]
cities = ["Brussels", "Leuven", "Paris", "Berlin", ""]

for i in range(1, 121):
    order_date = datetime(2026, 1, 1) + timedelta(days=random.randint(0, 30))
    ship_date = order_date + timedelta(days=random.randint(-3, 10))

    rows.append({
        "order_id": i if i != 10 else 9,
        "customer_id": random.choice([f"CUST{i:04d}", "", None]),
        "product_id": f"PROD{random.randint(1, 30):03d}",
        "order_date": order_date.strftime("%Y-%m-%d"),
        "ship_date": ship_date.strftime("%Y-%m-%d"),
        "quantity": random.choice([1, 2, 3, 4, -1, None]),
        "unit_price": random.choice([9.99, 19.99, 49.99, 99.99, -5, None]),
        "discount": random.choice([0, 0.05, 0.1, 0.2, 1.2, None]),
        "payment_method": random.choice(payment_methods),
        "country": random.choice(countries),
        "city": random.choice(cities),
        "status": random.choice(statuses),
    })

df = pd.DataFrame(rows)
df.to_csv("data/input/realtime/orders_dirty.csv", index=False)

print("Created data/input/realtime/orders_dirty.csv")
print("Shape:", df.shape)
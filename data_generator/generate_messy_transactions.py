from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta

fake = Faker()

def generate_data(n=100000):
    base_time = datetime.now() - timedelta(days=30)
    data = []

    for _ in range(n):
        record = {
            "transaction_id": fake.uuid4(),
            "customer_id": str(random.randint(1, 1000)),
            "product_id": random.randint(1, 1000),
            "store_id": random.randint(1, 50),
            "timestamp": base_time + timedelta(minutes=random.randint(0, 50000)),
            "quantity": random.randint(1, 5),
            "price": round(random.uniform(5, 500), 2),
            "payment_method": random.choice(["card", "cash", "online"]),
        }

        # 🟡 Inject duplicates (same transaction_id, slightly different values)
        if random.random() < 0.05:
            dup = record.copy()
            dup["price"] = round(random.uniform(5, 500), 2)  # changed value
            data.append(dup)

        # 🔴 Inject nulls
        if random.random() < 0.03:
            record["price"] = None

        # 🔴 Inject bad data
        if random.random() < 0.02:
            record["price"] = -random.uniform(1, 100)  # negative price

        # 🔴 Late arriving data (older timestamps)
        if random.random() < 0.05:
            record["timestamp"] = base_time - timedelta(days=random.randint(1, 10))

        # Format timestamp for Spark
        record["timestamp"] = record["timestamp"].isoformat()

        data.append(record)

    return pd.DataFrame(data)


if __name__ == "__main__":
    df = generate_data()
    df.to_csv("transactions_raw.csv", index=False)
    print("Generated messy transactions_raw.csv")

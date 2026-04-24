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

        # inject duplicates
        if random.random() < 0.03:
            data.append(record)

        # inject nulls
        if random.random() < 0.02:
            record["price"] = None

        data.append(record)

    return pd.DataFrame(data)


if __name__ == "__main__":
    df = generate_data()
    df.to_csv("transactions_raw.csv", index=False)
    print("Generated transactions_raw.csv")

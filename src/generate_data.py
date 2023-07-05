import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

def main():
    # Create a Faker instance
    fake = Faker()

    # Create a DataFrame of 100 random sales data
    data = pd.DataFrame([(i, fake.catch_phrase(), random.randint(1, 100), round(random.uniform(5.0, 50.0), 2),
             fake.date_between(start_date='-1y', end_date='today'), fake.company(),
             fake.name()) for i in range(1, 101)], columns=["id", "product", "quantity", "price", "date", "store", "salesperson"])

    # Save the DataFrame to a CSV file
    data.to_csv("data/sales_data.csv", index=False)

if __name__ == "__main__":
    main()

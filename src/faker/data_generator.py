import csv
import os
import random
from datetime import datetime, timedelta

import pandas as pd

from faker import Faker

# Crie uma instância do Faker para gerar dados brasileiros
fake = Faker('pt_BR')


def generate_customers_data(dir_path='.', num_rows=50):
    with open(
        os.path.join(dir_path, 'customers_data.csv'), 'w', newline=''
    ) as csvfile:
        fieldnames = [
            'customer_id',
            'customer_name',
            'age',
            'gender',
            'city',
            'join_date',
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for _ in range(num_rows):
            writer.writerow(
                {
                    'customer_id': fake.unique.random_number(digits=5),
                    'customer_name': fake.name(),
                    'age': fake.random_int(min=18, max=80),
                    'gender': fake.random_element(elements=('M', 'F')),
                    'city': fake.city(),
                    'join_date': fake.date_between(
                        start_date='-5y', end_date='today'
                    ).isoformat(),
                }
            )


def generate_products_data(dir_path='.', num_rows=10):
    with open(
        os.path.join(dir_path, 'products_data.csv'), 'w', newline=''
    ) as csvfile:
        fieldnames = [
            'product_id',
            'product_name',
            'product_category',
            'product_price',
            'supplier',
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for _ in range(num_rows):
            writer.writerow(
                {
                    'product_id': fake.unique.random_number(digits=5),
                    'product_name': fake.catch_phrase(),
                    'product_category': fake.random_element(
                        elements=('Eletrônicos', 'Móveis', 'Alimentos')
                    ),
                    'product_price': fake.pydecimal(
                        right_digits=2,
                        positive=True,
                        min_value=1,
                        max_value=100,
                    ),
                    'supplier': fake.company(),
                }
            )


def generate_store_data(dir_path='.', num_rows=10):
    with open(
        os.path.join(dir_path, 'stores_data.csv'), 'w', newline=''
    ) as csvfile:
        fieldnames = [
            'store_id',
            'store_name',
            'store_location',
            'store_type',
            'store_size',
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for _ in range(num_rows):
            writer.writerow(
                {
                    'store_id': fake.unique.random_number(digits=5),
                    'store_name': fake.company(),
                    'store_location': fake.city(),
                    'store_type': fake.random_element(
                        elements=('Física', 'Online')
                    ),
                    'store_size': fake.random_int(min=500, max=5000),
                }
            )


def generate_salesperson_data(dir_path='.', num_rows=10):
    with open(
        os.path.join(dir_path, 'salespeople_data.csv'), 'w', newline=''
    ) as csvfile:
        fieldnames = [
            'salesperson_id',
            'salesperson_name',
            'salesperson_age',
            'salesperson_experience',
            'salesperson_department',
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for _ in range(num_rows):
            writer.writerow(
                {
                    'salesperson_id': fake.unique.random_number(digits=5),
                    'salesperson_name': fake.name(),
                    'salesperson_age': fake.random_int(min=18, max=60),
                    'salesperson_experience': fake.random_int(min=1, max=40),
                    'salesperson_department': fake.random_element(
                        elements=('Eletrônicos', 'Móveis', 'Alimentos')
                    ),
                }
            )


def generate_sales_data(dir_path='.'):
    # Load the previously generated data
    customers_df = pd.read_csv(os.path.join(dir_path, 'customers_data.csv'))
    products_df = pd.read_csv(os.path.join(dir_path, 'products_data.csv'))
    stores_df = pd.read_csv(os.path.join(dir_path, 'stores_data.csv'))
    salespersons_df = pd.read_csv(os.path.join(dir_path, 'salespeople_data.csv'))

    # Initialize empty lists for each sales data column
    sales_id = []
    customer_id = []
    product_id = []
    store_id = []
    salesperson_id = []
    quantity = []
    sales_date = []
    sales_amount = []

    # Generate a random number of sales for each customer
    for i in range(customers_df.shape[0]):
        num_sales = random.randint(1, 10)
        for _ in range(num_sales):
            sales_id.append(len(sales_id) + 1)
            customer_id.append(customers_df.at[i, 'customer_id'])
            product_id.append(random.choice(products_df['product_id'].tolist()))
            store_id.append(random.choice(stores_df['store_id'].tolist()))
            salesperson_id.append(random.choice(salespersons_df['salesperson_id'].tolist()))
            quantity.append(random.randint(1, 3))
            sales_date.append(fake.date_between(start_date='-1y', end_date='today').isoformat())
            sales_amount.append(random.uniform(10.0, 1000.0))

    # Create a DataFrame for the sales data
    sales_df = pd.DataFrame({
        'sales_id': sales_id,
        'fk_customer_id': customer_id,
        'fk_product_id': product_id,
        'fk_store_id': store_id,
        'fk_salesperson_id': salesperson_id,
        'quantity': quantity,
        'sales_date': sales_date,
        'sales_amount': sales_amount
    })

    # Write the sales data to a CSV file
    sales_df.to_csv(os.path.join(dir_path, 'sales_data.csv'), index=False)

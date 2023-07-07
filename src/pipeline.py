import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create Spark session
spark = SparkSession.builder.master('local').appName('salesAnalysis').getOrCreate()

# Define the path to the output directory
output_dir = 'data/results'

# Read the data from CSV files
sales_df = spark.read.csv('data/sales_data.csv', header=True, inferSchema=True)
products_df = spark.read.csv('data/products_data.csv', header=True, inferSchema=True)
stores_df = spark.read.csv('data/stores_data.csv', header=True, inferSchema=True)
salespeople_df = spark.read.csv('data/salespeople_data.csv', header=True, inferSchema=True)
customers_df = spark.read.csv('data/customers_data.csv', header=True, inferSchema=True)

# Join the dataframes
analysis_datafame = sales_df.join(products_df, sales_df.fk_product_id == products_df.product_id) \
    .join(stores_df, sales_df.fk_store_id == stores_df.store_id) \
    .join(salespeople_df, sales_df.fk_salesperson_id == salespeople_df.salesperson_id) \
    .join(customers_df, sales_df.fk_customer_id == customers_df.customer_id)

# Análise de desempenho do vendedor
salespeople_performance = analysis_datafame.groupBy('salesperson_name') \
    .agg(F.format_number(F.sum('sales_amount'), 0).alias('total_sales'),
         F.approx_count_distinct('customer_id').alias('num_customers')) \
    .orderBy(F.col('total_sales').desc())

# Save salespeople_performance as CSV
salespeople_performance.write.csv(os.path.join(output_dir, 'salespeople_performance'), header=True, mode='overwrite')


# Análise de produtos
product_performance = analysis_datafame.groupBy('product_name') \
    .agg(F.format_number(F.sum('sales_amount'), 0).alias('total_sales'),
         F.approx_count_distinct('customer_id').alias('num_customers')) \
    .orderBy(F.col('total_sales').desc())

# Save product_analysis as CSV
product_performance.write.csv(os.path.join(output_dir, 'product_performance'), header=True, mode='overwrite')

# Análise de lojas
store_analysis = analysis_datafame.groupBy('store_name') \
    .agg(F.format_number(F.sum('sales_amount'), 0).alias('total_sales'),
         F.approx_count_distinct('customer_id').alias('num_customers')) \
    .orderBy(F.col('total_sales').desc())

# Save store_analysis as CSV
store_analysis.write.csv(os.path.join(output_dir, 'store_analysis'), header=True, mode='overwrite')

# Análise de clientes
customer_analysis = analysis_datafame.groupBy('customer_name') \
    .agg(F.format_number(F.sum('sales_amount'), 0).alias('total_purchase'),
         F.approx_count_distinct('product_id').alias('num_products')) \
    .orderBy(F.col('total_purchase').desc())

# Save customer_analysis as CSV
customer_analysis.write.csv(os.path.join(output_dir, 'customer_analysis'), header=True, mode='overwrite')


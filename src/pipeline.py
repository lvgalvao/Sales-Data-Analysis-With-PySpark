import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum

# Inicia uma sessão Spark
spark = SparkSession.builder.appName('salesAnalysis').getOrCreate()

# Define o caminho para a pasta de dados
data_dir = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', 'data'
)

# Carrega todos os arquivos CSV
sales_df = spark.read.csv(
    os.path.join(data_dir, 'sales_data.csv'), header=True, inferSchema=True
)
products_df = spark.read.csv(
    os.path.join(data_dir, 'products_data.csv'), header=True, inferSchema=True
)
stores_df = spark.read.csv(
    os.path.join(data_dir, 'stores_data.csv'), header=True, inferSchema=True
)
salespeople_df = spark.read.csv(
    os.path.join(data_dir, 'salespeople_data.csv'),
    header=True,
    inferSchema=True,
)
customers_df = spark.read.csv(
    os.path.join(data_dir, 'customers_data.csv'), header=True, inferSchema=True
)


# Junta todos os dados em um único DataFrame
df = (
    sales_df.join(
        products_df, sales_df.fk_product_id == products_df.product_id
    )
    .join(stores_df, sales_df.fk_store_id == stores_df.store_id)
    .join(
        salespeople_df,
        sales_df.fk_salesperson_id == salespeople_df.salesperson_id,
    )
    .join(customers_df, sales_df.fk_customer_id == customers_df.customer_id)
)

# Análise de desempenho do vendedor
salespeople_performance = df.groupBy('salesperson_name').agg(
    sum('sales_amount').alias('total_sales'),
    count('customer_id').alias('num_customers'),
)
salespeople_performance.show()

# Análise de produtos
product_analysis = df.groupBy('product_name').agg(
    sum('sales_amount').alias('total_sales'),
    count('customer_id').alias('num_customers'),
)
product_analysis.show()

# Análise de lojas
store_analysis = df.groupBy('store_name').agg(
    sum('sales_amount').alias('total_sales'),
    count('customer_id').alias('num_customers'),
)
store_analysis.show()

# Análise de clientes
customer_analysis = df.groupBy('customer_name').agg(
    sum('sales_amount').alias('total_sales'),
    count('product_id').alias('num_products'),
)
customer_analysis.show()

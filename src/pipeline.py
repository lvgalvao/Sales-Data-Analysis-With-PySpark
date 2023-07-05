from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, to_date
import os

def pipeline():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("SalesDataPipeline") \
        .getOrCreate()

    # Load the sales data CSV file
    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
    sales_data_file = os.path.join(data_dir, 'sales_data.csv')

    sales_data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(sales_data_file)

    # Convert the 'date' column from string to date type
    sales_data = sales_data.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

    # Create a new column 'revenue' which is 'quantity' * 'price'
    sales_data = sales_data.withColumn("revenue", expr("quantity * price"))

    # Create a new DataFrame that aggregates the total revenue by store
    total_revenue_by_store = sales_data.groupBy("store").sum("revenue")

    # Create a new DataFrame that aggregates the total revenue by salesperson
    total_revenue_by_salesperson = sales_data.groupBy("salesperson").sum("revenue")

    # Save the transformed DataFrames as Delta tables
    sales_data.write.format("csv").mode("overwrite").option("header", "true").save("output/sales_data")
    total_revenue_by_store.write.format("csv").mode("overwrite").option("header", "true").save("output/total_revenue_by_store")
    total_revenue_by_salesperson.write.format("csv").mode("overwrite").option("header", "true").save("output/total_revenue_by_salesperson")

if __name__ == "__main__":
    pipeline()

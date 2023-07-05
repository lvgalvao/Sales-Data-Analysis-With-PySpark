from pyspark.sql import SparkSession


def create_hello_world_df(spark):
    return spark.createDataFrame([('Hello, World!',)], ['greeting'])


def save_df_to_csv(df, path):
    df.repartition(1).write.csv(f'{path}hello_world', header=True)


def pipeline():
    spark = SparkSession.builder.appName('HelloWorld').getOrCreate()
    df = create_hello_world_df(spark)
    df.show()
    save_df_to_csv(df, 'output/')


if __name__ == '__main__':
    pipeline()

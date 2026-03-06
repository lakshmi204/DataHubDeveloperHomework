from pyspark.sql import SparkSession

def get_spark():

    spark = (
        SparkSession.builder
        .appName("ACME Big Data Pipeline")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    return spark
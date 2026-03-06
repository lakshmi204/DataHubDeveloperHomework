def read_location(spark, path):

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(path)
    )

    return df


def read_product(spark, path):

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(path)
    )

    return df


def read_transactions(spark, path):

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(path)
    )

    return df
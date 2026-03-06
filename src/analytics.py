from pyspark.sql.functions import sum, avg, col, when
from pyspark.sql.window import Window
from pyspark.sql.functions import rank


def store_performance(df):

    store_sales = (
        df.groupBy("province", "store_location_key")
        .agg(sum("sales").alias("total_sales"))
    )

    province_avg = (
        store_sales.groupBy("province")
        .agg(avg("total_sales").alias("province_avg_sales"))
    )

    result = store_sales.join(province_avg, "province")

    return result


def loyalty_vs_nonloyalty(df):

    df2 = df.withColumn(
        "collector_key_num",
        col("collector_key").cast("double")
    )

    loyalty_df = (
        df2.withColumn(
            "customer_type",
            when(col("collector_key_num") > 0, "LOYALTY")
            .otherwise("NON_LOYALTY")
        )
    )

    result = (
        loyalty_df.groupBy("customer_type")
        .agg(
            sum("sales").alias("total_sales"),
            sum("units").alias("total_units")
        )
    )

    return result


def top_5_stores_by_province(df):

    store_sales = (
        df.groupBy("province", "store_location_key")
        .agg(sum("sales").alias("sales"))
    )

    windowSpec = Window.partitionBy("province").orderBy(col("sales").desc())

    result = (
        store_sales
        .withColumn("rank", rank().over(windowSpec))
        .filter(col("rank") <= 5)
    )

    return result


def top_10_category_by_department(df):

    cat_sales = (
        df.groupBy("department", "category")
        .agg(sum("sales").alias("sales"))
    )

    windowSpec = Window.partitionBy("department").orderBy(col("sales").desc())

    result = (
        cat_sales
        .withColumn("rank", rank().over(windowSpec))
        .filter(col("rank") <= 10)
    )

    return result
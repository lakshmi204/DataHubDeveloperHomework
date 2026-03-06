from pyspark.sql.functions import (
    col,
    coalesce,
    lit,
    to_date,
    when
)

def cleanse_transactions(trans_df):

    # remove malformed rows
    df = trans_df.filter(col("store_location_key").rlike("^[0-9]+$"))

    # handle multiple date formats
    df = df.withColumn(
        "trans_dt",
        when(
            col("trans_dt").rlike("^[0-9]{2}-[0-9]{2}-[0-9]{4}$"),
            to_date(col("trans_dt"), "dd-MM-yyyy")
        ).when(
            col("trans_dt").rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2}$"),
            to_date(col("trans_dt"), "yyyy-MM-dd")
        )
    )

    df = (
        df
        .withColumn("sales", col("sales").cast("double"))
        .withColumn("units", col("units").cast("int"))
    )

    df = (
        df
        .withColumn("sales", coalesce(col("sales"), lit(0.0)))
        .withColumn("units", coalesce(col("units"), lit(0)))
    )

    return df


def build_master_dataset(trans_df, location_df, product_df):

    df = (
        trans_df
        .join(location_df, "store_location_key", "left")
        .join(product_df, "product_key", "left")
    )

    return df
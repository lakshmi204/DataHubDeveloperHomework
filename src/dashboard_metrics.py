from pyspark.sql.functions import (
    col,
    sum,
    countDistinct,
    when,
    year,
    to_timestamp
)

def dashboard_metrics(df):

    df2 = df.withColumn(
        "trans_dt_parsed",
        when(
            col("trans_dt").rlike("^[0-9]{2}-[0-9]{2}-[0-9]{4}$"),
            to_timestamp(col("trans_dt"), "dd-MM-yyyy")
        ).when(
            col("trans_dt").rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2}$"),
            to_timestamp(col("trans_dt"), "yyyy-MM-dd")
        )
    )

    df3 = df2.withColumn(
        "customer_type",
        when(col("collector_key").cast("double") > 0, "LOYALTY")
        .otherwise("NON_LOYALTY")
    )

    metrics = (
        df3.groupBy(
            "customer_type",
            year(col("trans_dt_parsed")).alias("year")
        )
        .agg(
            sum("sales").alias("sales"),
            sum("units").alias("units"),
            countDistinct("trans_key").alias("transactions"),
            countDistinct("collector_key").alias("collectors")
        )
    )

    return metrics
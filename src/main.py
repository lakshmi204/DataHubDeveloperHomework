from config import get_spark
from ingestion import read_location, read_product, read_transactions
from transformation import cleanse_transactions, build_master_dataset
from analytics import (
    store_performance,
    loyalty_vs_nonloyalty,
    top_5_stores_by_province,
    top_10_category_by_department
)
from dashboard_metrics import dashboard_metrics


def main():

    spark = get_spark()

    location_path = "data/raw/location.csv"
    product_path = "data/raw/product.csv"
    fact_path = "data/raw/fact/*.csv"

    location_df = read_location(spark, location_path)
    product_df = read_product(spark, product_path)

    trans_df = read_transactions(spark, fact_path)

    clean_trans = cleanse_transactions(trans_df)

    master_df = build_master_dataset(clean_trans, location_df, product_df)

    store_perf = store_performance(master_df)
    loyalty = loyalty_vs_nonloyalty(master_df)
    top_stores = top_5_stores_by_province(master_df)
    top_categories = top_10_category_by_department(master_df)

    dashboard = dashboard_metrics(master_df)

    store_perf.write.mode("overwrite").option("header", True).csv("output/store_performance")

    loyalty.write.mode("overwrite").option("header", True).csv("output/analytics/loyalty_vs_nonloyalty")

    top_stores.write.mode("overwrite").option("header", True).csv("output/analytics/top_stores_by_province")

    top_categories.write.mode("overwrite").option("header", True).csv("output/analytics/top_categories_by_department")

    dashboard.write.mode("overwrite").option("header", True).csv("output/analytics/dashboard_metrics")


if __name__ == "__main__":
    main()
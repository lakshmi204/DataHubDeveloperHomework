# ACME Big Data Pipeline

## Overview

```
This project builds a simple Spark-based data pipeline to ingest, clean, and analyze ACME grocery store transaction data. The pipeline reads raw CSV files, prepares the data, joins dimension tables, and generates analytics insights.
```

## Steps

```
1. **Data Ingestion** – Load raw CSV files (location, product, and transaction facts) using Spark.
2. **Data Cleansing** – Fix date formats, cast numeric fields, remove malformed rows, and replace null sales/units with 0.
3. **Data Integration** – Join transaction data with location and product datasets.
4. **Analytics** – Generate insights:

   * Store performance by province
   * Loyalty vs non-loyalty customer performance
   * Top 5 stores by province
   * Top 10 product categories by department
5. **Output** – Results are written to the `output/` folder as CSV files.
```

## Project Structure

```
DataHubDeveloperHomework/
│
├── data/
│   └── raw/
│       ├── location.csv
│       ├── product.csv
│       └── fact/
│           ├── trans_fact_1.csv
│           ├── trans_fact_2.csv
│           ├── ...
│           └── trans_fact_10.csv
│
├── src/
│   ├── config.py
│   ├── ingestion.py
│   ├── transformation.py
│   ├── analytics.py
│   ├── dashboard_metrics.py
│   └── main.py
│
├── output/
│
└── README.md
```

## Run the Pipeline

```
python src/main.py
```

## Output

The pipeline generates analytics datasets:

```
* store_performance
* loyalty_vs_nonloyalty
* top_stores_by_province
* top_categories_by_department
* dashboard_metrics
```

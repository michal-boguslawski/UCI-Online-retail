import os
import pathlib
import sys
import boto3
import pyspark.sql.functions as f


# modify sys.path BEFORE imports that depend on it
dir_path = pathlib.Path(__file__).parent.parent
sys.path.append(str(dir_path))

from helper_functions import SparkSessionManager  # noqa: E402


if __name__ == "__main__":
    s3_bucket = os.getenv("S3_BUCKET_NAME")
    spark_session_manager = SparkSessionManager()
    s3 = boto3.client("s3")

    tables_list = set()
    response = s3.list_objects_v2(Bucket=s3_bucket)
    for obj in response["Contents"]:
        tmp_object_name = obj["Key"].split("/")
        if tmp_object_name[0] == "silver":
            tables_list.add(tmp_object_name[1])

    for table in tables_list:
        s3_path = f"s3a://{s3_bucket}/silver/{table}/v1/"
        spark_session_manager.add_df(s3_path=s3_path, df_name=table)

    spark_dfs = spark_session_manager.get_df()
    order_df = spark_dfs["fact_order"]
    order_detail_df = spark_dfs["fact_order_detail"]
    customer_df = spark_dfs["dim_customer"]
    product_df = spark_dfs["dim_product"]
    cancellation_df = spark_dfs["fact_cancellation"]

    totals_df = order_df \
        .join(order_detail_df, on="DimOrderKey") \
        .groupBy(
            "Country",
            f.year("InvoiceDate").alias("Year"),
            f.month("InvoiceDate").alias("Month")
        ) \
        .agg(
            f.round(
                f.sum(f.col("Quantity") * f.col("UnitPrice"))
                , 2)
            .alias("TotalRevenue"),
            f.countDistinct(f.col("InvoiceNo")).alias("TotalOrders"),
            (
                f.round(
                    f.sum(f.col("Quantity") * f.col("UnitPrice"))
                    / f.countDistinct(f.col("InvoiceNo")).alias("TotalOrders")
                    , 2)
            ).alias("AverageOrderValue"),
            f.countDistinct(f.col("DimCustomerKey")).alias("TotalCustomers")
        )

    write_s3_path = f"s3a://{s3_bucket}/gold/sales/fact_sales_summary/v1/"
    totals_df.write.mode("overwrite").csv(write_s3_path, header="true")

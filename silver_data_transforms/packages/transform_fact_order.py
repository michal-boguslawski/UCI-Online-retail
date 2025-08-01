import pyspark.sql.functions as f
from .helper_function import create_session_and_load_bucket


def transform_fact_order(s3_bucket: str):
    # Initialize Spark session and Read Avro data
    spark, df = create_session_and_load_bucket(s3_bucket)

    df_flat = df.filter(
        ( ~f.col("InvoiceNo").startswith("C") )
        & ( ~f.col("InvoiceNo").startswith("A") )
    ).withColumn(
        "CustomerId",
        f.when(f.isnan(f.col("CustomerID")) | f.col("CustomerID").isNull(), "Unknown")
        .otherwise(f.col("CustomerID"))
    ).select(
        "InvoiceNo",
        "InvoiceDate",
        "Country",
        "CustomerId",
        f.lit("Kafka").alias("SourceSystem")
    )

    df_final = df_flat.withColumns(
        {
            "DimCustomerKey":
                f.hash(f.lit("Customer"), f.col("CustomerId")),
            "DimOrderKey":
                f.hash(f.lit("Order"), f.col("InvoiceNo"), f.col("SourceSystem"))
        }
    )
    df_final = df_final.select(
        "DimOrderKey",
        "InvoiceNo",
        "InvoiceDate",
        "DimCustomerKey",
        "Country",
        "SourceSystem"
    )

    (
        df_final
        .write
        .mode("overwrite")
        .parquet(f"s3a://{s3_bucket}/silver/fact_order/v1")
    )
    spark.stop()

import pyspark.sql.functions as f
from .helper_function import create_session_and_load_bucket


def transform_fact_order_detail(s3_bucket: str):
    # Initialize Spark session and Read Avro data
    spark, df = create_session_and_load_bucket(s3_bucket)

    df_flat = df.filter(
        ( ~f.col("InvoiceNo").startswith("A") )
    ).withColumn(
        "OrderType",
        f.when(( f.col("InvoiceNo").startswith("C") ), f.lit("Cancellation"))
        .otherwise(f.lit("Order"))
    ).withColumn(
        "Order",
        f.explode("OrderList")
    ).select(
        "InvoiceNo",
        "OrderType",
        "Order.StockCode",
        "Order.Quantity",
        "Order.UnitPrice",
        f.lit("Kafka").alias("SourceSystem")
    ).filter(
        f.col("UnitPrice") != 0
    ).withColumn(
        "StockCode",
        f.upper(f.col("StockCode"))
    )

    df_final = df_flat.withColumns(
        {
            "DimProductKey":
                f.hash(f.lit("Product"), f.col("StockCode")),
            "DimOrderKey":
                f.hash(f.col("OrderType"), f.col("InvoiceNo"), f.col("SourceSystem"))
        }
    )

    df_final = df_final.select(
        "DimOrderKey",
        "DimProductKey",
        "Quantity",
        "UnitPrice",
        "OrderType"
    )

    (
        df_final
        .write
        .mode("overwrite")
        .parquet(f"s3a://{s3_bucket}/silver/fact_order_detail/v1")
    )

    spark.stop()

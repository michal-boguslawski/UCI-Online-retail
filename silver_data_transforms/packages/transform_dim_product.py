import pyspark.sql.functions as f
from .helper_function import create_session_and_load_bucket


def transform_dim_product(s3_bucket: str):
    # Initialize Spark session and Read Avro data
    spark, df = create_session_and_load_bucket(s3_bucket)

    df_flat = df.filter(
        ( ~f.col("InvoiceNo").startswith("A") )
    ).withColumn("Order", f.explode("OrderList")).select(
        "Order.StockCode",
        "Order.Description",
        "Order.UnitPrice",
        f.lit("Kafka").alias("SourceSystem")
    )

    df_flat = df_flat.filter(
        f.col("UnitPrice") != 0
    ).withColumn("StockCode", f.upper(f.col("StockCode")))

    df_added_columns = df_flat.withColumns(
        {
            "TrimmedStockCode": f.col("StockCode").substr(1, 5),
            "IsInt": f.col("StockCode").substr(1, 5).cast("int").isNotNull(),
        }
    ).withColumns(
        {
            "ProductModel": f.when(
                f.col("IsInt"),
                f.col("TrimmedStockCode")
            ).otherwise(f.col("StockCode")),
            "ProductVersion": f.when(
                ( f.col("IsInt") )
                & ( f.length("StockCode") > 5 ),
                f.col("StockCode").substr(6, 100)
            ),
            "DimProductKey": f.hash(f.lit("Product"), f.col("StockCode")),
            "IsSpecial":
                ( ~f.col("IsInt") )
                & ( ~f.col("StockCode").startswith("DCGS") )
                & ( ~f.col("StockCode").startswith("GIFT") )
        }
    )

    df_final = df_added_columns.orderBy(
        f.col("DimProductKey"),
        f.col("Description").isNull().asc()
    ).dropDuplicates(["DimProductKey"])
    df_final = df_final.select(
        "DimProductKey",
        "StockCode",
        "Description",
        "ProductModel",
        "ProductVersion",
        "IsSpecial",
        "SourceSystem"
    )

    # Show a few rows (default is 20, or you can set it explicitly)
    df_final.show(5, truncate=False)  # Show 5 rows, no truncation

    df_final.write.mode("overwrite").parquet(f"s3a://{s3_bucket}/silver/dim_product/v1")
    spark.stop()

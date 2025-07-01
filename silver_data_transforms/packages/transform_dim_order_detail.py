from pyspark.sql import SparkSession
import pyspark.sql.functions as f


def transform_dim_order_detail(s3_bucket: str):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Read Avro from S3") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()

    # S3 path to the Avro files
    s3_path = f"s3a://{s3_bucket}/bronze/kafka/sales_events/"

    # Read Avro data
    df = spark.read.format("avro").load(s3_path)

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

    df_final = df_flat.withColumn("DimProductKey", f.hash(f.lit("Product"), f.col("StockCode")))
    df_final = df_final.withColumn("DimOrderKey", f.hash(f.col("OrderType"), f.col("InvoiceNo"), f.col("SourceSystem")))
    df_final = df_final.select(
        "DimOrderKey",
        "DimProductKey",
        "Quantity",
        "UnitPrice",
        "OrderType"
    )

    df_final.write.mode("overwrite").parquet(f"s3a://{s3_bucket}/silver/dim_orderdetail/v1")
    spark.stop()

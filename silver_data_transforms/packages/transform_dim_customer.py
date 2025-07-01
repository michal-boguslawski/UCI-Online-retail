from pyspark.sql import SparkSession
import pyspark.sql.functions as f


def transform_dim_customer(s3_bucket: str):
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
        "CustomerId", 
        f.when(f.isnan(f.col("CustomerID")) | f.col("CustomerID").isNull(), "Unknown")
        .otherwise(f.col("CustomerID"))
    ).select(
        "CustomerId",
        f.lit("Kafka").alias("SourceSystem")
    )

    df_final = df_flat.withColumn("DimCustomerKey", f.hash(f.lit("Customer"), f.col("CustomerId")))
    df_final = df_final.dropDuplicates(["DimCustomerKey"])
    df_final = df_final.select(
        "DimCustomerKey",
        "CustomerId",
        "SourceSystem"
    )

    df_final.write.mode("overwrite").parquet(f"s3a://{s3_bucket}/silver/dim_customer/v1")
    spark.stop()

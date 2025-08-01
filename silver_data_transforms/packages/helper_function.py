from pyspark.sql import SparkSession


def create_session_and_load_bucket(s3_bucket: str):
    # Initialize Spark session
    spark = (
        SparkSession.builder
        .appName("Read Avro from S3")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        .getOrCreate()
    )

    # S3 path to the Avro files
    s3_path = f"s3a://{s3_bucket}/bronze/kafka/sales_events/"

    # Read Avro data
    df = spark.read.format("avro").load(s3_path)
    return spark, df

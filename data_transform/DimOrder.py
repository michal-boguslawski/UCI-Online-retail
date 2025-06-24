from pyspark.sql import SparkSession
import pyspark.sql.functions as f

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Read Avro from S3") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

# S3 path to the Avro files
s3_path = "s3a://bucket-uci-retail-project-1/bronze/kafka/sales_events/"

# Read Avro data
df = spark.read.format("avro").load(s3_path)

df_flat = df.filter(
    ( ~f.col("InvoiceNo").startswith("C") )
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

df_final = df_flat.withColumn("DimCustomerKey", f.hash(f.lit("Customer"), f.col("CustomerId")))
df_final = df_final.withColumn("DimOrderKey", f.hash(f.lit("Order"), f.col("InvoiceNo"), f.col("SourceSystem")))
df_final.select(
    "DimOrderKey",
    "InvoiceNo",
    "InvoiceDate",
    "DimCustomerKey",
    "Country",
    "SourceSystem"
)

df_final.write.mode("overwrite").parquet("s3a://bucket-uci-retail-project-1/silver/dim_order/v1")

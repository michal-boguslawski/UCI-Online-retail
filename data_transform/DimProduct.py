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


df_flat = df.withColumn("Order", f.explode("OrderList")).select(
    "Order.StockCode",
    "Order.Description",
    "Order.UnitPrice",
    f.lit("Kafka").alias("SourceSystem")
)

df_flat = df_flat.filter(
    f.col("UnitPrice") != 0
)

df_final = df_flat.withColumn("DimProductKey", f.hash(f.lit("Product"), f.col("StockCode")))
df_final = df_final.dropDuplicates(["DimProductKey"])
df_final = df_final.select(
    "DimProductKey",
    "StockCode",
    "Description",
    "SourceSystem"
)

df_final.write.mode("overwrite").parquet("s3a://bucket-uci-retail-project-1/silver/dim_product/v1")

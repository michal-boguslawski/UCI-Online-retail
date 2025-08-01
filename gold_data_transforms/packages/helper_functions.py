from pyspark.sql import SparkSession


class SparkSessionManager:
    def __init__(self, app_name: str = "Spark batch processing S3"):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config(
                "spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem"
            ) \
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
            ) \
            .getOrCreate()
        self.spark_dfs = {}

    def add_df(self, s3_path, df_name: str, format: str = "parquet"):
        df = self.spark.read.format(format).load(s3_path)
        print(df.head())
        self.spark_dfs[df_name] = df

    def get_df(self) -> dict:
        return self.spark_dfs

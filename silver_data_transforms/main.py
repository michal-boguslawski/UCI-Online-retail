import os
from packages.transform_fact_cancellation import transform_fact_cancellation
from packages.transform_dim_customer import transform_dim_customer
from packages.transform_fact_order import transform_fact_order
from packages.transform_fact_order_detail import transform_fact_order_detail
from packages.transform_dim_product import transform_dim_product


if __name__ == "__main__":
    s3_bucket = os.getenv("S3_BUCKET_NAME")
    transform_dim_product(s3_bucket=s3_bucket)
    transform_dim_customer(s3_bucket=s3_bucket)
    transform_fact_order(s3_bucket=s3_bucket)
    transform_fact_cancellation(s3_bucket=s3_bucket)
    transform_fact_order_detail(s3_bucket=s3_bucket)

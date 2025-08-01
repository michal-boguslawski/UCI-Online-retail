import os
from packages.create_objects import create_or_clear_bucket


if __name__ == "__main__":
    region = os.getenv("S3_REGION")
    bucket_name = os.getenv("S3_BUCKET_NAME")
    response = create_or_clear_bucket(bucket_name=bucket_name, region=region)
    print(response)

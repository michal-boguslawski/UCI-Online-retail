import os
from packages.create_objects import create_or_clear_bucket


if __name__ == "__main__":
    # Read the secrets from the files
    with open('/run/secrets/aws_access_key_id') as f:
        access_key = f.read().strip()

    with open('/run/secrets/aws_secret_access_key') as f:
        secret_key = f.read().strip()

    # Set them as environment variables
    os.environ['AWS_ACCESS_KEY_ID'] = access_key
    os.environ['AWS_SECRET_ACCESS_KEY'] = secret_key

    region = os.getenv("S3_REGION")
    bucket_name = os.getenv("S3_BUCKET_NAME")
    response = create_or_clear_bucket(bucket_name=bucket_name, region=region)
    print(response)

import boto3


def create_or_clear_bucket(bucket_name: str, region: str = "eu-north-1", if_clear: bool = True) -> list | dict | None:
    """
    creates a bucket or if exists clears its content
    """
    s3_client = boto3.client("s3", region_name=region)
    list_buckets = s3_client.list_buckets()
    response = None
    if bucket_name not in [bucket["Name"] for bucket in list_buckets["Buckets"]]:
        location = {'LocationConstraint': region}
        response = s3_client.create_bucket(Bucket=bucket_name,
                                           CreateBucketConfiguration=location)
        print(f"Bucket created {response}", flush=True)
    elif if_clear:
        print(f"S3 bucket named {bucket_name} already exists", flush=True)
        bucket = boto3.resource("s3").Bucket(bucket_name)
        response = bucket.objects.all().delete()
        print(f"Objects removed {response}", flush=True)
    return response 
        
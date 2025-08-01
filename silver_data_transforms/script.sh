spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.hadoop.fs.s3a.endpoint=s3.eu-north-1.amazonaws.com \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
    --packages org.apache.spark:spark-avro_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 \
    main.py

spark-submit --packages org.apache.spark:spark-avro_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 DimProduct.py
spark-submit --packages org.apache.spark:spark-avro_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 DimCustomer.py
spark-submit --packages org.apache.spark:spark-avro_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 DimOrder.py
spark-submit --packages org.apache.spark:spark-avro_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 DimCancellation.py
spark-submit --packages org.apache.spark:spark-avro_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 DimOrderDetail.py
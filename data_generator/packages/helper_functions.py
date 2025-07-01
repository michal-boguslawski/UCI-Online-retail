import os
import time
import pandas as pd
from confluent_kafka.admin import AdminClient, KafkaException


current_file = os.path.abspath(__file__)
current_dir = os.path.dirname(current_file)

def delivery_report(err, msg):
    """
    write on delivery report
    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
    
def get_data(file_name):
    """
    function that retrive data from xlsx
    """
    dtypes_dict = {
            "InvoiceNo": str,
            "StockCode": str,
            "Description": str,
            "Quantity": str,
            "UnitPrice": str,
            "CustomerID": str, 
            "Country": str
        }
    
    sheet_name = "Online Retail"
    
    df = pd.read_excel(
        file_name,
        sheet_name=sheet_name,
        dtype=dtypes_dict,
        parse_dates=["InvoiceDate"]
    )
    df = df.sort_values(
        by=["InvoiceDate", "InvoiceNo"], 
        ascending=True
    ).reset_index(
        drop=True
    )
    df['InvoiceDate'] = df['InvoiceDate'].astype(str)
    
    for _, row in df.iterrows():
        yield row.to_dict()
        
def check_kafka_connection(BOOTSTRAP_SERVERS: str, RETRY_INTERVAL: float = 5) -> bool:
    """
    check if kafka connection is available
    """

    conf = {"bootstrap.servers": BOOTSTRAP_SERVERS}

    while True:
        try:
            admin = AdminClient(conf)
            # Force connection and metadata fetch
            admin.list_topics(timeout=5)
            print("✅ Kafka broker is up and reachable.", flush=True)
            break
        except KafkaException as e:
            print("❌ Kafka broker is not reachable yet. Retrying in 5 seconds...", flush=True)
            print(f"Error: {e}", flush=True)
            time.sleep(RETRY_INTERVAL)
            
    return True


avro_schema_str = """
{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "InvoiceNo", "type": "string"},
    {"name": "InvoiceDate", "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
        }
    },
    {"name": "CustomerID", "type": "string"},
    {"name": "Country", "type": "string"},
    {"name": "OrderList", "type": {
      "type": "array",
      "items": {
        "type": "record",
        "name": "OrderItem",
        "fields": [
          {"name": "StockCode", "type": "string"},
          {"name": "Description", "type": ["null", "string"]},
          {"name": "Quantity", "type": "int"},
          {"name": "UnitPrice", "type": "float"}
        ]
      }
    }}
  ]
}
"""
        
if __name__ == "__main__":
    df = get_data()
    for _ in range(5):
        print(next(df))
    
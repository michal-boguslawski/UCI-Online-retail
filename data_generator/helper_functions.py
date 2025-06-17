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
    
def get_data():
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
    file_name = current_dir + "/Online Retail.xlsx"
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
        
if __name__ == "__main__":
    df = get_data()
    for _ in range(5):
        print(next(df))
    
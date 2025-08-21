import os
import requests
from zipfile import ZipFile
import time
import pandas as pd
from typing import Generator
from confluent_kafka.admin import AdminClient, KafkaException


current_file = os.path.abspath(__file__)
current_dir = os.path.dirname(current_file)


class KafkaConnector:
    def __init__(self, bootstrap_servers: str):
        self.conf = {
            "bootstrap.servers": bootstrap_servers
        }
        self.kafka_admin_client = None

    def _kafka_set_connection(self) -> None:
        """establish connection to kafka client"""
        self.kafka_admin_client = AdminClient(self.conf)

    def _check_kafka_connection(self) -> bool:
        """check if kafka connection is available"""
        return check_kafka_connection(
            bootstrap_servers=self.conf["bootstrap.servers"]
        )


def download_and_extract_file(url: str) -> list[str]:
    """
    download and extract archive to data folder and
    returns absolute paths to all extracted files
    """

    os.makedirs("data", exist_ok=True)
    # Step 1: Download the file
    zip_file = url.split("/")[-1]
    file_path = os.path.join("data", zip_file)

    response = requests.get(url)
    with open(file_path, 'wb') as f:
        f.write(response.content)
    print("Data is downloaded", flush=True)

    # Step 2: Unzip the file
    with ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall('data')  # Extracts to the current directory
    print("Archive is unzipped", flush=True)

    list_extracted_files = os.listdir("data")
    list_extracted_files.remove(zip_file)

    files_path = [os.path.abspath(
        os.path.join("data", file)
    ) for file in list_extracted_files]

    return files_path


def delivery_report(err, msg):
    """
    write on delivery report
    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def get_data(file_path: str) -> Generator:
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
        file_path,
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


def check_kafka_connection(
    bootstrap_servers: str,
    retry_interval: int | float = 5.
) -> bool:
    """
    check if kafka connection is available
    """

    conf = {"bootstrap.servers": bootstrap_servers}

    while True:
        try:
            admin = AdminClient(conf)
            # Force connection and metadata fetch
            admin.list_topics(timeout=5)
            print("✅ Kafka broker is up and reachable.", flush=True)
            break
        except KafkaException as e:
            print(
                "❌ Kafka broker is not reachable yet. Retrying in 5 seconds...",
                flush=True
            )
            print(f"Error: {e}", flush=True)
            time.sleep(retry_interval)

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

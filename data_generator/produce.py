from time import sleep
import os
from datetime import datetime
from json import dumps
from confluent_kafka import Producer

from helper_functions import get_data, delivery_report, check_kafka_connection

# configuration
topic = os.getenv("KAFKA_TOPIC")
if topic is None:
    raise ValueError("Environment variable KAFKA_TOPIC is not set!")
old_invoice_no = '-1'
verbose = False

conf = {
    "bootstrap.servers": "kafka-1:9092,kafka-2:9092"
}

_ = check_kafka_connection(conf["bootstrap.servers"])

producer = Producer(**conf)

# get data generator
data = get_data()

order = None
print("Starts producing data", flush=True)

while True:
    try:
        row = next(data)
        curr_invoice_no = row["InvoiceNo"]
        
        order_element = {
            "StockCode": row["StockCode"],
            "Description": str(row["Description"]),
            "Quantity": int(row["Quantity"]),
            "UnitPrice": float(row["UnitPrice"])
        }
        
        if old_invoice_no != curr_invoice_no:
            if order:
                producer.produce(
                    topic=topic,
                    key=curr_invoice_no,
                    value=dumps(order),
                    on_delivery=delivery_report,
                )
                if verbose:
                    producer.poll(0)
                sleep(0.1)
                
            order = {
                "InvoiceNo": curr_invoice_no,
                "InvoiceDate": datetime.strptime(row["InvoiceDate"], '%Y-%m-%d %H:%M:%S').isoformat(),
                "CustomerID": str(row["CustomerID"]),
                "Country": row["Country"],
                "OrderList": [order_element, ],
            }
            if old_invoice_no == -1:
                print(order, flush=True)
            old_invoice_no = curr_invoice_no
            
        else:
            order["OrderList"].append(order_element)
            
    except Exception as e:
        print(e)
        print(order)
        break

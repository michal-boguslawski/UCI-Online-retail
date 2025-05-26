from confluent_kafka import Producer
from helper_functions import get_data, delivery_report
from json import dumps
from time import sleep

# configuration
conf = {
    "bootstrap.servers": 'kafka-1:9092, kafka-2:9092'
}
topic = "sales_events"
old_invoice_no = '-1'
verbose = False

# get data generator
data = get_data()

# create producer
producer = Producer(conf)

while True:
    row = next(data)
    curr_invoice_no = row["InvoiceNo"]
    key = curr_invoice_no + "|" + row["StockCode"]
    if old_invoice_no != curr_invoice_no:
        sleep(1)
        old_invoice_no = curr_invoice_no
    producer.produce(
        topic=topic,
        key=key,
        value=dumps(row),
        on_delivery=delivery_report
    )
    if verbose:
        producer.poll(1.0)

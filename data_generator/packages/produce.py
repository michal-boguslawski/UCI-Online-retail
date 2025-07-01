from time import sleep
from datetime import datetime
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import Producer

from .helper_functions import get_data, delivery_report, \
    check_kafka_connection, avro_schema_str


def produce_to_kafka(
    topic: str,
    schema_registry: str,
    kafka_cluster: str,
    file_name: str
):

    # configuration
    if topic is None:
        raise ValueError("Environment variable KAFKA_TOPIC is not set!")
    old_invoice_no = "-1"
    verbose = False

    schema_registry_conf = {"url": schema_registry}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    subject = f"{topic}-value"
    schema = Schema(avro_schema_str, "AVRO")
    schema_id = schema_registry_client.register_schema(subject, schema)
    print(f"Schema registered with id: {schema_id}", flush=True)

    avro_serializer = AvroSerializer(
        schema_str=avro_schema_str,
        schema_registry_client=schema_registry_client,
        to_dict=lambda obj, ctx: obj  # assume obj is already a dict
    )

    producer_conf = {
        "bootstrap.servers": kafka_cluster,
    }

    _ = check_kafka_connection(producer_conf["bootstrap.servers"])

    producer = Producer(producer_conf)

# get data generator

    data = get_data(file_name)

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
                        value=avro_serializer(
                            order,
                            SerializationContext(topic, MessageField.VALUE)
                        ),
                        on_delivery=delivery_report,
                    )
                    if verbose:
                        producer.poll(0)
                    sleep(0.01)

                order = {
                    "InvoiceNo": curr_invoice_no,
                    "InvoiceDate": int(
                        datetime.strptime(
                            row["InvoiceDate"],
                            '%Y-%m-%d %H:%M:%S'
                        ).timestamp() * 1000
                    ),
                    "CustomerID": str(row["CustomerID"]),
                    "Country": row["Country"],
                    "OrderList": [order_element, ],
                }
                if old_invoice_no == "-1":
                    print(order, flush=True)
                old_invoice_no = curr_invoice_no

            else:
                order["OrderList"].append(order_element)

        except Exception as e:
            print(e, flush=True)
            print(order, flush=True)
            break

        finally:
            producer.flush()

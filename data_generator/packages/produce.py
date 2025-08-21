from time import sleep
from datetime import datetime
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import Producer

from .helper_functions import get_data, delivery_report, \
    avro_schema_str, KafkaConnector


class KafkaProducer(KafkaConnector):
    def __init__(self, schema_registry: str, bootstrap_servers: str):
        super().__init__(bootstrap_servers=bootstrap_servers)
        self.schema_registry = schema_registry
        self.bootstrap_servers = bootstrap_servers
        self.avro_serializer = None
        self.producer = None
        self.data = None

    def _register_schema(self, topic: str):
        schema_registry_conf = {"url": self.schema_registry}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        subject = f"{topic}-value"
        schema = Schema(avro_schema_str, "AVRO")
        schema_id = schema_registry_client.register_schema(subject, schema)
        print(f"Schema registered with id: {schema_id}", flush=True)

        self.avro_serializer = AvroSerializer(
            schema_str=avro_schema_str,
            schema_registry_client=schema_registry_client,
            to_dict=lambda obj, ctx: obj  # assume obj is already a dict
        )

    def _set_producer(self):
        producer_conf = {
            "bootstrap.servers": self.bootstrap_servers,
        }
        self.producer = Producer(producer_conf)

    def _get_data(self, file_path: str) -> None:
        """preprocess data into generator ready for kafka producing"""
        self.data = get_data(file_path)

    def _produce_to_kafka(
        self,
        key: str,
        data: dict,
        topic: str,
        time_sleep: float = 0.01,
        verbose: bool = False
    ):
        self.producer.produce(
            topic=topic,
            key=key,
            value=self.avro_serializer(
                data,
                SerializationContext(topic, MessageField.VALUE)
            ),
            on_delivery=delivery_report if verbose else None,
        )
        if verbose:
            self.producer.poll(0)
        sleep(time_sleep)

    def _produce_preprocess_row(
        self,
        row: dict,
        old_invoice_no: str,
        order: dict,
        topic: str,
        verbose: bool = False
    ):
        """group data items into orders"""
        curr_invoice_no = row["InvoiceNo"]

        order_element = {
            "StockCode": row["StockCode"],
            "Description": str(row["Description"]),
            "Quantity": int(row["Quantity"]),
            "UnitPrice": float(row["UnitPrice"])
        }

        if old_invoice_no != curr_invoice_no:
            if order:
                self._produce_to_kafka(
                    key=order["InvoiceNo"],
                    data=order,
                    topic=topic,
                    verbose=verbose
                )

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
        return old_invoice_no, order

    def produce(self, topic: str, verbose: bool = False):
        print("Starts producing data", flush=True)
        old_invoice_no = "-1"
        order = None
        while True:
            try:
                row = next(self.data)
                old_invoice_no, order = self._produce_preprocess_row(
                    row=row,
                    old_invoice_no=old_invoice_no,
                    order=order,
                    topic=topic,
                    verbose=verbose
                )

            except Exception as e:
                print(e, flush=True)
                print(order, flush=True)
                break

            finally:
                self.producer.flush()

    def run(
        self,
        topic: str,
        file_path: str,
        verbose: bool = False
    ):
        self._register_schema(topic=topic)
        self._set_producer()
        self._get_data(file_path=file_path)
        self.produce(topic=topic, verbose=verbose)

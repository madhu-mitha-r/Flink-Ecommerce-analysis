import os

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ORDERS_NUM_PARTITIONS = int(os.getenv("ORDERS_KAFKA_NUM_PARTITIONS", "3"))
ORDERS_REPLICATION_FACTOR = int(os.getenv("ORDERS_KAFKA_REPLICATION_FACTOR", "1"))

PAYMENTS_NUM_PARTITIONS = int(os.getenv("PAYMENTS_KAFKA_NUM_PARTITIONS", "3"))
PAYMENTS_REPLICATION_FACTOR = int(os.getenv("PAYMENTS_KAFKA_REPLICATION_FACTOR", "1"))

PRODUCT_CDC_NUM_PARTITIONS = int(os.getenv("PRODUCT_CDC_KAFKA_NUM_PARTITIONS", "1"))
PRODUCT_CDC_REPLICATION_FACTOR = int(os.getenv("PRODUCT_CDC_KAFKA_REPLICATION_FACTOR", "1"))

ORDERS_TOPIC = "orders"
PAYMENTS_TOPIC = "payments"
PRODUCT_CDC_TOPIC = "product_cdc"


def create_topics():
    admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    orders_topic = NewTopic(
        ORDERS_TOPIC, num_partitions=ORDERS_NUM_PARTITIONS, replication_factor=ORDERS_REPLICATION_FACTOR
    )
    payments_topic = NewTopic(
        PAYMENTS_TOPIC, num_partitions=PAYMENTS_NUM_PARTITIONS, replication_factor=PAYMENTS_REPLICATION_FACTOR
    )
    product_cdc_topic = NewTopic(
        PRODUCT_CDC_TOPIC,
        num_partitions=PRODUCT_CDC_NUM_PARTITIONS,
        replication_factor=PRODUCT_CDC_REPLICATION_FACTOR,
        config={"cleanup.policy": "compact"},
    )

    futures = admin_client.create_topics([orders_topic, payments_topic, product_cdc_topic])
    for topic, future in futures.items():
        try:
            future.result()
            print(f"topic created: {topic}", flush=True)
        except KafkaException as e:
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"topic already exists: {topic}", flush=True)
            else:
                raise


if __name__ == "__main__":
    create_topics()

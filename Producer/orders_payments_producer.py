import heapq
import itertools
import json
import random
import time
import uuid

from confluent_kafka import Producer
from faker import Faker

from create_topics import BOOTSTRAP_SERVERS, ORDERS_TOPIC, PAYMENTS_TOPIC, PRODUCT_CDC_TOPIC, create_topics

ORDERS_PER_SECOND = 5
PAYMENT_TIMEOUT_SEC = 60
NUM_CUSTOMERS = 200
PRODUCT_CHANGE_INTERVAL_SEC = 30
MIN_CATALOG_SIZE = 5

SCENARIO_WEIGHTS = {
    "normal": 82,
    "abandoned": 5,
    "late_payment": 4,
    "duplicate_payment": 3,
    "amount_mismatch": 2,
    "double_charge": 2,
    "payment_before_order": 1,
    "orphan_payment": 1,
}

PAYMENT_STATUS_WEIGHTS = {"SUCCESS": 80, "FAILED": 10, "PENDING": 3,"CANCELED":7}
PAYMENT_METHODS = ["CREDIT_CARD", "DEBIT_CARD", "UPI", "PAYPAL", "WALLET"]

PRODUCT_CHANGE_WEIGHTS = {"u": 80, "c": 15, "d": 5}
CATEGORIES = ["electronics", "fashion", "stationery", "grocery", "sports", "home"]
BRANDS = ["Logitech", "Anker", "Nike", "Levis", "Classmate", "Pilot", "Lavazza", "Lipton", "Decathlon", "Ikea"]

INITIAL_CATALOG = [
    {"productId": "p-101", "productName": "Wireless Mouse", "category": "electronics", "brand": "Logitech", "unitPrice": 24.99},
    {"productId": "p-102", "productName": "Mechanical Keyboard", "category": "electronics", "brand": "Logitech", "unitPrice": 89.00},
    {"productId": "p-103", "productName": "USB-C Charger", "category": "electronics", "brand": "Anker", "unitPrice": 19.49},
    {"productId": "p-201", "productName": "Running Shoes", "category": "fashion", "brand": "Nike", "unitPrice": 74.95},
    {"productId": "p-202", "productName": "Denim Jacket", "category": "fashion", "brand": "Levis", "unitPrice": 59.90},
    {"productId": "p-301", "productName": "Notebook A5", "category": "stationery", "brand": "Classmate", "unitPrice": 3.50},
    {"productId": "p-302", "productName": "Gel Pen Pack", "category": "stationery", "brand": "Pilot", "unitPrice": 6.25},
    {"productId": "p-401", "productName": "Coffee Beans 1kg", "category": "grocery", "brand": "Lavazza", "unitPrice": 18.75},
    {"productId": "p-402", "productName": "Green Tea 100 bags", "category": "grocery", "brand": "Lipton", "unitPrice": 7.99},
    {"productId": "p-501", "productName": "Yoga Mat", "category": "sports", "brand": "Decathlon", "unitPrice": 29.00},
]

fake = Faker()
pending = []
sequence = itertools.count()
catalog = {}
new_product_ids = itertools.count(601)


def now_ms():
    #seconds to milliseconds
    return int(time.time() * 1000)


def weighted_choice(weights):
    return random.choices(list(weights), weights=list(weights.values()))[0]


def generate_order():
    products = random.sample(list(catalog.values()), k=random.randint(1, 4))
    items = [
        {"productId": product["productId"], "unitPrice": product["unitPrice"], "quantity": random.randint(1, 5)}
        for product in products
    ]
    total_amount = round(sum(item["quantity"] * item["unitPrice"] for item in items), 2)
    return {
        "orderId": f"ord-{uuid.uuid4().hex[:12]}",
        "customerId": f"cust-{random.randint(1, NUM_CUSTOMERS)}",
        "eventTime": now_ms(),
        "currency": "USD",
        "items": items,
        "totalAmount": total_amount,
        "shippingCountry": fake.country_code(),
    }


def generate_payment(order, delay_sec, amount=None, status=None):
    return {
        "orderId": order["orderId"],
        "transactionId": f"txn-{uuid.uuid4().hex[:12]}",
        "eventTime": order["eventTime"] + int(delay_sec * 1000),
        "method": random.choice(PAYMENT_METHODS),
        "status": status or weighted_choice(PAYMENT_STATUS_WEIGHTS),
        "amount": order["totalAmount"] if amount is None else amount,
        "currency": order["currency"],
    }


def product_cdc_event(op, before, after):
    return {"op": op, "before": before, "after": after, "ts_ms": now_ms()}


def send_product_snapshot(producer):
    for product in INITIAL_CATALOG:
        row = {**product, "updatedAt": now_ms()}
        catalog[row["productId"]] = row
        send(producer, PRODUCT_CDC_TOPIC, row["productId"], product_cdc_event("r", None, row))
    producer.flush(10)


def send_product_change(producer):
    op = weighted_choice(PRODUCT_CHANGE_WEIGHTS)
    if op == "d" and len(catalog) <= MIN_CATALOG_SIZE:
        op = "u"

    if op == "c":
        row = {
            "productId": f"p-{next(new_product_ids)}",
            "productName": fake.catch_phrase(),
            "category": random.choice(CATEGORIES),
            "brand": random.choice(BRANDS),
            "unitPrice": round(random.uniform(2, 150), 2),
            "updatedAt": now_ms(),
        }
        catalog[row["productId"]] = row
        send(producer, PRODUCT_CDC_TOPIC, row["productId"], product_cdc_event("c", None, row))
        return

    before = random.choice(list(catalog.values()))
    if op == "d":
        del catalog[before["productId"]]
        send(producer, PRODUCT_CDC_TOPIC, before["productId"], product_cdc_event("d", before, None))
        return

    field = random.choice(["category", "brand", "unitPrice"])
    if field == "unitPrice":
        new_value = round(before["unitPrice"] * random.uniform(0.8, 1.2), 2)
    else:
        new_value = random.choice([v for v in (CATEGORIES if field == "category" else BRANDS) if v != before[field]])
    after = {**before, field: new_value, "updatedAt": now_ms()}
    catalog[after["productId"]] = after
    send(producer, PRODUCT_CDC_TOPIC, after["productId"], product_cdc_event("u", before, after))


def schedule(delay_sec, topic, record):
    heapq.heappush(pending, (time.time() + delay_sec, next(sequence), topic, record))


def schedule_order_and_payment(producer):
    order = generate_order()
    scenario = weighted_choice(SCENARIO_WEIGHTS)

    if scenario == "payment_before_order":
        payment = generate_payment(order, delay_sec=random.uniform(1, 3))
        send(producer, PAYMENTS_TOPIC, payment["orderId"], payment)
        schedule(random.uniform(3, 6), ORDERS_TOPIC, order)
        return
    if scenario == "orphan_payment":
        payment = generate_payment(order, delay_sec=0, status="SUCCESS")
        send(producer, PAYMENTS_TOPIC, payment["orderId"], payment)
        return

    send(producer, ORDERS_TOPIC, order["orderId"], order)

    if scenario == "abandoned":
        return
    if scenario == "late_payment":
        delay = PAYMENT_TIMEOUT_SEC + random.uniform(5, 30)
        schedule(delay, PAYMENTS_TOPIC, generate_payment(order, delay))
        return
    if scenario == "amount_mismatch":
        delay = random.uniform(1, 10)
        wrong_amount = round(order["totalAmount"] * random.uniform(0.5, 0.95), 2)
        schedule(delay, PAYMENTS_TOPIC, generate_payment(order, delay, amount=wrong_amount))
        return
    if scenario == "double_charge":
        delay = random.uniform(1, 10)
        schedule(delay, PAYMENTS_TOPIC, generate_payment(order, delay, status="SUCCESS"))
        second_delay = delay + random.uniform(0.5, 3)
        schedule(second_delay, PAYMENTS_TOPIC, generate_payment(order, second_delay, status="SUCCESS"))
        return

    delay = random.uniform(1, 10)
    payment = generate_payment(order, delay)
    schedule(delay, PAYMENTS_TOPIC, payment)
    if scenario == "duplicate_payment":
        schedule(delay + random.uniform(0.5, 3), PAYMENTS_TOPIC, payment)


def send_due(producer):
    while pending and pending[0][0] <= time.time():
        _, _, topic, record = heapq.heappop(pending)
        send(producer, topic, record["orderId"], record)


def delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed key={msg.key()} topic={msg.topic()}: {err}", flush=True)
        return
    print(
        f"delivered topic={msg.topic()} partition={msg.partition()} offset={msg.offset()} key={msg.key().decode()}",
        flush=True,
    )


def send(producer, topic, key, record):
    value = json.dumps(record).encode()
    while True:
        try:
            producer.produce(topic, key=key.encode(), value=value, callback=delivery_report)
            break
        except BufferError:
            producer.poll(1)
    producer.poll(0)


def main():
    create_topics()
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 5,
        }
    )
    send_product_snapshot(producer)
    next_product_change = time.time() + PRODUCT_CHANGE_INTERVAL_SEC
    try:
        while True:
            schedule_order_and_payment(producer)
            send_due(producer)
            if time.time() >= next_product_change:
                send_product_change(producer)
                next_product_change = time.time() + PRODUCT_CHANGE_INTERVAL_SEC
            time.sleep(1 / ORDERS_PER_SECOND)
    except KeyboardInterrupt:
        print(f"stopping, {len(pending)} scheduled messages not sent", flush=True)
    finally:
        producer.flush(10)


if __name__ == "__main__":
    main()

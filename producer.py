
import json
import time
import uuid
import random
import argparse
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker

fake = Faker()


def create_producer():
    return KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5
    )


def generate_event():
    return {
        "event_id": f"evt-{uuid.uuid4().hex}",
        "order_id": f"ord-{random.randint(100, 999)}",
        "customer_id": f"cust-{random.randint(1, 100)}",
        "product_id": f"prod-{random.randint(1, 50)}",
        "quantity": random.randint(1, 10),
        "unit_price": random.randint(10000, 500000),
        "event_time": datetime.now(timezone.utc).isoformat()
    }


def main():
    parser = argparse.ArgumentParser(description="Kafka Order Producer")

    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="Number of events to generate"
    )

    parser.add_argument(
        "--rate",
        type=float,
        default=1.0,
        help="Events per second"
    )

    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Generate events continuously"
    )

    args = parser.parse_args()

    if args.count <= 0:
        parser.error("--count must be greater than zero")

    if args.rate <= 0:
        parser.error("--rate must be greater than zero")

    producer = create_producer()
    topic = "orders.events"

    print(f"Starting producer at {args.rate} events/sec")

    sent_count = 0

    try:
        while args.continuous or sent_count < args.count:
            event = generate_event()

            future = producer.send(topic, value=event)
            future.get(timeout=10)

            sent_count += 1
            print(f"Sent event {sent_count}: {event['event_id']}")

            time.sleep(1.0 / args.rate)

    except KeyboardInterrupt:
        print("\nProducer stopped by user.")

    finally:
        producer.flush()
        producer.close()

    print(f"Producer finished. Total events sent: {sent_count}")


if __name__ == "__main__":
    main()

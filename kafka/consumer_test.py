"""
kafka/consumer_test.py
-----------------------
Simple Kafka consumer to verify messages are flowing into the
`toll-transactions` topic. Useful for local debugging.

Author : Mayukh Ghosh | Roll No : 23052334
"""

import json
from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9092"
TOPIC        = "toll-transactions"
GROUP_ID     = "toll-debug-consumer"


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    print(f"👂 Listening on '{TOPIC}' (Ctrl+C to stop)...\n")
    try:
        for msg in consumer:
            event = msg.value
            print(
                f"[{event['timestamp']}] "
                f"Plaza={event['plaza_id']} | "
                f"Tag={event['tag_id']} | "
                f"Class={event['class_name']} | "
                f"Amount=₹{event['amount']} | "
                f"Status={event['status']}"
            )
    except KeyboardInterrupt:
        print("\n⛔ Consumer stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

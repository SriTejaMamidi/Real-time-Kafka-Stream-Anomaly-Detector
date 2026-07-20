#!/usr/bin/env python3
"""Generate and send synthetic metrics to Kafka."""

import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka_producer import KafkaMetricsProducer, generate_sample_metrics
import time
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Main function."""
    producer = KafkaMetricsProducer(brokers=["localhost:9092"])

    logger.info("Starting to send metrics to Kafka...")

    try:
        metric_count = 0
        for metric in generate_sample_metrics(num_samples=100000):
            if producer.send_metric(metric):
                metric_count += 1

                if metric_count % 1000 == 0:
                    logger.info(f"Sent {metric_count} metrics")

            time.sleep(0.01)  # ~100 metrics/sec

    except KeyboardInterrupt:
        logger.info("Stopped by user")
    finally:
        producer.close()
        logger.info(f"Total metrics sent: {metric_count}")


if __name__ == "__main__":
    main()

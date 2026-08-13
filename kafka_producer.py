"""
Kafka metrics producer for streaming data.
Sends time-series metrics to Kafka topic.
"""

import json
import time
import logging
from kafka import KafkaProducer
from datetime import datetime
import numpy as np

logger = logging.getLogger(__name__)


class KafkaMetricsProducer:
    """Produces metrics to Kafka topic."""

    def __init__(self, brokers, topic="metrics", max_retries=3):
        """Initialize Kafka producer.

        Args:
            brokers: List of broker addresses
            topic: Kafka topic to produce to
            max_retries: Max connection retries
        """
        self.brokers = brokers
        self.topic = topic
        self.max_retries = max_retries

        self.producer = KafkaProducer(
            bootstrap_servers=brokers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=max_retries,
            compression_type="snappy"
        )

        logger.info(f"Connected to Kafka brokers: {brokers}")

    def send_metric(self, metric_data: dict) -> bool:
        """Send metric to Kafka.

        Args:
            metric_data: Dictionary with metric data

        Returns:
            True if sent successfully
        """
        try:
            # Add timestamp if not present
            if "timestamp" not in metric_data:
                metric_data["timestamp"] = time.time()

            future = self.producer.send(self.topic, value=metric_data)
            record_metadata = future.get(timeout=10)

            logger.debug(
                f"Sent to {record_metadata.topic}:"
                f"[{record_metadata.partition}]"
                f"@{record_metadata.offset}"
            )

            return True

        except Exception as e:
            logger.error(f"Failed to send metric: {e}")
            return False

    def send_batch(self, metrics: list) -> int:
        """Send batch of metrics.

        Args:
            metrics: List of metric dictionaries

        Returns:
            Number of successfully sent metrics
        """
        sent_count = 0
        for metric in metrics:
            if self.send_metric(metric):
                sent_count += 1

        return sent_count

    def close(self):
        """Close producer connection."""
        self.producer.close()
        logger.info("Producer closed")


def generate_sample_metrics(num_samples: int = 100):
    """Generate sample metrics with occasional anomalies.

    Args:
        num_samples: Number of metrics to generate

    Yields:
        Dictionary with metric data
    """
    for i in range(num_samples):
        # Normal distribution
        cpu = np.random.normal(45, 15)
        memory = np.random.normal(60, 12)
        network = np.random.normal(3.5, 1.2)
        disk = np.random.normal(65, 10)

        # Inject anomalies (5% of time)
        if np.random.random() < 0.05:
            cpu = np.random.uniform(85, 99)
            memory = np.random.uniform(90, 98)
            network = np.random.uniform(8, 10)

        yield {
            "timestamp": time.time() + i,
            "source": f"server-{np.random.randint(1, 6):02d}",
            "cpu_usage": max(0, min(100, cpu)),
            "memory_usage": max(0, min(100, memory)),
            "network_io": max(0, network),
            "disk_io": max(0, min(100, disk))
        }

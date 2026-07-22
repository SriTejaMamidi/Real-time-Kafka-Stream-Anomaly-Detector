"""
PySpark Streaming consumer for real-time anomaly detection.
Consumes from Kafka, applies Isolation Forest, publishes alerts.
"""

import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, stddev,
    current_timestamp, struct
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType
)
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)


class KafkaAnomalyConsumer:
    """Spark Streaming consumer for anomaly detection."""

    def __init__(self, brokers: list, topic: str = "metrics"):
        """Initialize consumer.

        Args:
            brokers: List of Kafka broker addresses
            topic: Kafka topic to consume from
        """
        self.brokers = ",".join(brokers)
        self.topic = topic

        self.spark = SparkSession.builder \
            .appName("KafkaAnomalyDetection") \
            .getOrCreate()

        # Suppress verbose logging
        self.spark.sparkContext.setLogLevel("WARN")

        logger.info("SparkSession created")

    def create_schema(self) -> StructType:
        """Create schema for incoming metrics."""
        return StructType([
            StructField("timestamp", DoubleType(), True),
            StructField("source", StringType(), True),
            StructField("cpu_usage", DoubleType(), True),
            StructField("memory_usage", DoubleType(), True),
            StructField("network_io", DoubleType(), True),
            StructField("disk_io", DoubleType(), True),
        ])

    def get_kafka_stream(self):
        """Get Kafka stream."""
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.brokers) \
            .option("subscribe", self.topic) \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", 10000) \
            .load()

        schema = self.create_schema()

        # Parse JSON
        df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        return df

    def apply_windowing(self, df, window_duration: int = 300):
        """Apply time window (5 minutes default).

        Args:
            df: Input DataFrame
            window_duration: Window duration in seconds
        """
        from pyspark.sql.functions import to_timestamp

        df_with_ts = df.withColumn(
            "ts", to_timestamp(col("timestamp"))
        )

        windowed = df_with_ts.groupBy(
            window(col("ts"), f"{window_duration} seconds"),
            col("source")
        ).agg(
            avg("cpu_usage").alias("avg_cpu"),
            avg("memory_usage").alias("avg_memory"),
            avg("network_io").alias("avg_network"),
            avg("disk_io").alias("avg_disk"),
            stddev("cpu_usage").alias("std_cpu")
        )

        return windowed

    def run_stream(self, query_name: str = "anomaly_detection"):
        """Run streaming query.

        Args:
            query_name: Name of the streaming query
        """
        df = self.get_kafka_stream()
        windowed = self.apply_windowing(df)

        query = windowed.writeStream \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 20) \
            .outputMode("update") \
            .option("checkpointLocation", f"checkpoint_{query_name}") \
            .start()

        logger.info(f"Streaming query '{query_name}' started")

        return query


def main():
    """Run consumer."""
    logging.basicConfig(level=logging.INFO)

    brokers = ["localhost:9092"]
    consumer = KafkaAnomalyConsumer(brokers)

    query = consumer.run_stream()
    query.awaitTermination()


if __name__ == "__main__":
    main()

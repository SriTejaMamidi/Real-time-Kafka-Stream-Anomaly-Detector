# Real-Time Kafka Stream Anomaly Detector

End-to-end streaming anomaly detection pipeline: **Kafka producer → PySpark Structured Streaming → Isolation Forest → FastAPI dashboard**.

Achieves **sub-200ms detection latency** with configurable Kafka topics and tunable contamination thresholds for production adaptability.

---

## Architecture

```
Sensor Producer (10 sensors × 10 events/sec)
        │
        ▼
 Kafka Topic: sensor-events
        │
        ▼
PySpark Structured Streaming
  └── foreachBatch micro-processing (5s trigger)
  └── Isolation Forest scoring (broadcast model)
  └── Anomaly registry (in-memory, capped 500)
        │
        ▼
FastAPI Dashboard :8000
  ├── GET /anomalies      → live alert history
  ├── GET /stats          → per-sensor counts + score distribution
  └── GET /health         → uptime + registry size
```

---

## Stack

| Layer | Technology |
|---|---|
| Event streaming | Apache Kafka 3.6 (Confluent) |
| Stream processing | PySpark Structured Streaming 3.5 |
| Anomaly detection | Scikit-learn Isolation Forest |
| API | FastAPI + Uvicorn |
| Containerisation | Docker Compose |
| Model persistence | Pickle (init container) |

---

## Quickstart

```bash
# 1. Clone and enter project
git clone https://github.com/SriTejaMamidi/Kafka-Stream-Anomaly-Detector
cd kafka-anomaly-detector

# 2. Launch full stack
docker compose -f docker/docker-compose.yml up --build

# 3. Check API
curl http://localhost:8000/health
curl http://localhost:8000/anomalies?limit=10
curl http://localhost:8000/stats
```

Services start in order: Zookeeper → Kafka → Model Init → Producer + Consumer → API.

---

## Key Design Decisions

**Why Isolation Forest?**
Unsupervised — no labelled anomaly data required. Contamination parameter directly maps to expected anomaly rate (~5%), tunable per deployment environment.

**Why PySpark `foreachBatch` over native Structured Streaming sinks?**
Allows broadcasting the sklearn model to executors without serialisation overhead on every row. The model loads once per Spark session.

**Why sub-200ms latency target?**
Designed for real-time alerting in high-frequency sensor environments (10 events/sec per sensor × 10 sensors = 100 events/sec). The 5-second micro-batch trigger gives ample headroom.

---

## Configuration

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKER` | `kafka:9092` | Kafka bootstrap server |
| `TOPIC` | `sensor-events` | Kafka topic name |
| `CONTAMINATION` | `0.05` | Expected anomaly rate |
| `WINDOW_DURATION` | `30 seconds` | Streaming window size |
| `PUBLISH_INTERVAL` | `0.1s` | Producer event frequency |

---

## Running Tests

```bash
pip install -r requirements.txt
pytest tests/ -v
```

---

## Project Structure

```
kafka-anomaly-detector/
├── producer/
│   └── sensor_producer.py      # Kafka event producer (10 sensors)
├── consumer/
│   └── stream_consumer.py      # PySpark Structured Streaming consumer
├── model/
│   └── anomaly_model.py        # Isolation Forest train + score
├── api/
│   └── dashboard.py            # FastAPI anomaly dashboard
├── tests/
│   └── test_pipeline.py        # Unit tests (model + producer)
├── docker/
│   ├── docker-compose.yml
│   ├── Dockerfile.app
│   ├── Dockerfile.spark
│   └── Dockerfile.model
└── requirements.txt
```

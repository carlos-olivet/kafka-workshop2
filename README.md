# Advanced Kafka Workshop: Schemas, Connect, and Streaming

Welcome to the Data Engineering Kafka Workshop! In this session, we move beyond basic producer/consumer scripts and build a production-grade event pipeline using the Schema Registry, Kafka Connect, and Apache Flink.

## 🏗 System Architecture

* **Producers:** Python FastAPI (Async, Avro Serialized)
* **Broker:** 3-Node Kafka Cluster (KRaft Mode - No Zookeeper)
* **Governance:** Confluent Schema Registry
* **Integration:** Kafka Connect (Sinks to MongoDB & Snowflake)
* **Processing:** Apache Flink (Windowed Aggregations)

## 🚀 Prerequisites

* Docker Desktop (Allocated Memory: 8GB+)
* Python 3.9+
* `pip install requests` (For the load generator)

## 🛠 Quick Start

**1. Start the Infrastructure**
```bash
docker-compose up -d --build
```



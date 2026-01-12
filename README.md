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




```bash
chmod +x setup.sh && ./setup.sh
```

Troubleshooting & Useful Commands
Here is a cheat sheet for managing your workshop environment.

1. Stopping & Resetting
If you want to stop the cluster or do a "nuclear reset" (useful if Kafka gets stuck).

Stop/Pause (Keep Data):

Bash

docker-compose stop
Tear Down (Delete Containers, Keep Data):

Bash

docker-compose down
The "Nuclear Clean" (Delete Everything + Data Volumes): Use this if you see "Cluster ID mismatch" errors or want to start fresh.

Bash

docker-compose down -v
docker volume prune -f
2. Service Management
Sometimes you only need to restart one component, not the whole cluster.

Restart only the Sales API (e.g., after changing Python code):

Bash

docker-compose restart sales-api
If you changed requirements.txt or Dockerfile, use:

Bash

docker-compose up -d --build --no-deps sales-api
Restart only Kafka Connect:

Bash

docker-compose restart kafka-connect
3. Debugging & Logs
If a service isn't working, the logs will tell you why.

Check API Logs (Real-time):

Bash

docker logs -f sales-api
Check Connector Logs (Did the data land in Mongo?):

Bash

docker logs kafka-connect
Check Broker Logs (Is the cluster healthy?):

Bash

docker logs broker-1
4. Manual Operations
Manually Create a Topic:

Bash

docker exec broker-1 kafka-topics --create \
  --topic test-topic \
  --bootstrap-server broker-1:29092 \
  --partitions 1 \
  --replication-factor 1
List All Topics:

Bash

docker exec broker-1 kafka-topics --list --bootstrap-server broker-1:29092
🧪 Workshop Exercises
Exercise 1: The Pipeline Check
Run the load generator.

Open Kafka UI and verify 3 partitions are active.

Open Mongo Express, select sales_db, and confirm records exist.

Exercise 2: Schema Evolution
Scenario: Marketing wants to add promotion_code to the sales events.

Update schemas/purchase_event.avsc to add the field (ensure default: null).

Update sales_api/models.py.

Restart the API (docker-compose restart sales-api) and verify that existing consumers (Mongo) do not break.

Exercise 3: Real-Time Aggregation
Scenario: Create a dashboard showing Revenue per Store every minute.

We will use Flink SQL to consume the raw_sale_events_topic.

Apply a TUMBLE window of 1 minute.

Sink the results to store_revenue_output.


| Service          | URL                              | Usage                                   |
|------------------|----------------------------------|-----------------------------------------|
| Kafka UI         | http://localhost:8080           | View Topics, Messages, and Partitions  |
| Schema Registry  | http://localhost:8080           | (Inside Kafka UI) View Avro Schemas    |
| Mongo Express    | http://localhost:8084           | View Sink Data in MongoDB              |
| Flink Dashboard  | http://localhost:8082           | Monitor Streaming Jobs                 |
| Sales API Docs   | http://localhost:8000/docs      | Swagger UI for the Producer            |

docker-compose down -v


docker logs -f broker-1


/nuke
# Stop containers and remove volumes
docker-compose down -v

# Prune any lingering volume data to be 100% sure
docker volume prune -f


/just api
# Recreate just the sales-api container with the new volume config
docker-compose up -d sales-api

# Check the logs immediately
docker logs -f sales-api


# Repo structure

kafka-workshop-2/
├── docker-compose.yml           # The main infrastructure definition
├── connect/
│   ├── Dockerfile               # Custom image to install Mongo/Snowflake plugins
│   ├── mongo-sink.json          # Configuration for MongoDB Sink
│   └── snowflake-sink.json      # Configuration for Snowflake Sink
├── schemas/
│   └── purchase_event.avsc      # Avro Schema for the sales event
├── flink/
│   └── fraud_detection.sql      # Example Flink SQL job for stream processing
├── sales_api/
│   ├── Dockerfile               # API Container definition
│   ├── main.py                  # FastAPI Application
│   ├── models.py                # Pydantic Models
│   ├── producer.py              # Kafka Producer logic with Schema Registry
│   └── requirements.txt         # Python dependencies
└── testing/
    └── load_generator.py        # Script to send 1, X, or X+Bad events
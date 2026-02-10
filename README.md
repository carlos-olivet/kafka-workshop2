# Kafka Workshop 2: Repository Overview and Setup Guide

Welcome to the Data Engineering Kafka Workshop! In this session, we move beyond basic producer/consumer scripts and build a production-grade event pipeline using the Schema Registry, Kafka Connect, and Apache Flink.

The main learning outcomes of this workshop are:

- Understand what a production level Kafka Environment looks like when including
    - **Kafka UI** for governance, managing Topics, schemas, producers, consumers, kafka connectors
    - **Kafka Connect** for data source and sinking (Mongo and Snowflake here specially)
    - **Confluent Schema manager** for schema registry and evolution 
    - **Flink** for data transformations on streams. Example is creating a table of 
    - **Data Producers** not just sending data but also using the schema registry to ensure data quality
---

## 📂 Repository Structure
```text
kafka-workshop-2/
├── connect/
│   ├── Dockerfile               # Custom image to install Mongo/Snowflake plugins
│   ├── mongo-sink.json          # Configuration for MongoDB Sink
│   └── snowflake-sink.json      # Configuration for Snowflake Sink (not set up here as it requires a real Snowflake account)
├── docker-compose.yml           # The main infrastructure definition
├── flink/
│   └── sql/
│       └── store_aggregation.sql  # Flink SQL job for aggregating store revenue
├── python_consumer/
│   └── requirements.txt         # Optional consumer dependencies, not yet part of workshop 2
├── sales-api/
│   ├── Dockerfile               # API Container definition
│   ├── main.py                  # FastAPI Application
│   ├── models.py                # Pydantic Model describing a sale event
│   ├── producer.py              # Kafka Producer logic which uses Schema Registry
│   └── requirements.txt         # Python dependencies
├── schemas/
│   └── purchase_event.avsc      # Avro Schema for the sales event
├── scripts/
│   └── deploy_schema.py         # Script to deploy Avro schema to Schema Registry
├── testing/
│   └── load-generator.py        # Script to send 1, X, or X+Bad events into the Kafka Pipeline
├── setup.sh                     # Main setup script for the workshop, this will create all the needed infrastructure
```
---

## 🏗 System Architecture

The system architecture is defined in the [docker-compose.yml](docker-compose.yml) file. Below is an overview of the components:

- **Producers:**
  - **Sales API:** A Python FastAPI application that produces sales events to Kafka. Events are serialized using Avro and validated against the Schema Registry.
  
- **Kafka Cluster:**
  - A 3-node Kafka cluster running in KRaft mode (no Zookeeper).
    - 3 KRaft controllers (controller-1..3) — Kafka control plane
    - 3 Kafka brokers (broker-1..3) — data plane (PLAINTEXT listeners)
  - Topics:
    - `raw_sale_events_topic`: Stores raw sales events.
    - `store_revenue_output`: Stores aggregated revenue data for each store.

- **Schema Management:**
  - **Confluent Schema Registry:** Manages Avro schemas for Kafka topics.

- **Integration:**
  - **Kafka Connect:** Configured with MongoDB and Snowflake sink connectors to store sales data in external systems.

- **Stream Processing:**
  - **Apache Flink:** Processes sales events in real-time, performing windowed aggregations and writing results back to Kafka.

- **Governance:**
  - **Kafka UI:** Provides a web interface to monitor Kafka topics, messages, and partitions.
  - **Mongo Express:** Allows viewing and managing data in MongoDB.
  - **Flink Dashboard:** Monitors Flink streaming jobs.
    - Job Manager: This is the UI dashboard itself that manages different Flink task managers
    - Task Manager: This is a an orchestrator for any created Flink tasks, each task manager is assigned a specific ammount of resources.

## Creating all the needed infrastructure:
This repo is set up for anyone to be able to easily spin up all the neded infrastructure simply by running the following command.
```bash
./setup.sh
```

The following section breaks down the file so users can also understand each step of the following 

## Setup.sh — step-by-step mapping (snippets and explanation)

Note: the code snippets are taken from setup.sh and grouped by the script's logical steps so you can compare easily.

### Step 0 - Setting up helper function to check service status: wait_for_service
```bash
wait_for_service() {
    local url=$1
    local name=$2
    local max_retries=30
    local count=0
    echo -n "Waiting for $name..."
    until curl --output /dev/null --silent --head --fail "$url"; do
        printf '.'
        count=$((count+1))
        if [ $count -ge $max_retries ]; then
            echo -e "\n${RED}Timeout waiting for $name!${NC}"
            return 1
        fi
        sleep 5
    done
    echo -e " ${GREEN}OK!${NC}"
}
```
Purpose: generic HTTP health-check used throughout the script.

---

### Step 1 — Check Docker is running
```bash
if ! docker info > /dev/null 2>&1; then
  echo -e "${RED}Error: Docker is not running.${NC}"
  exit 1
fi
```
Purpose: fail fast if Docker isn't available.

---

### Step 2 — Build Docker images
```bash
echo -e "${YELLOW}Building Docker images...${NC}"
docker-compose build
```
Purpose: build/rebuild images to reflect local changes.

---

### Step 3 — Start containers
```bash
echo -e "${YELLOW}Starting containers...${NC}"
docker-compose up -d
```
Purpose: bring up the whole stack in detached mode.

---

### Step 4 — Verify service health
```bash
wait_for_service "http://localhost:8080" "Kafka UI"
wait_for_service "http://localhost:8081" "Schema Registry"
wait_for_service "http://localhost:8083" "Kafka Connect"
wait_for_service "http://localhost:8084" "Mongo Express"
wait_for_service "http://localhost:8085" "Flink Dashboard"
wait_for_service "http://localhost:8000/docs" "Sales API"
```
Purpose: ensure key UIs and the Sales API are reachable before progressing.

---

### Step 5 — Deploy Avro schema to Schema Registry
```bash
docker-compose run --rm sales-api python /app/deploy_schema.py
docker-compose restart sales-api
wait_for_service "http://localhost:8000/docs" "Sales API"
```
Purpose: register the Avro schema (subject: `raw_sale_events_topic-value`) and restart the API so it picks up schema/code changes.

---

### Step 6 — Create Kafka topics
```bash
docker exec broker-1 kafka-topics --create --if-not-exists \
    --topic raw_sale_events_topic \
    --bootstrap-server broker-1:9092 \
    --partitions 3 \
    --replication-factor 3

docker exec broker-1 kafka-topics --create --if-not-exists \
    --topic store_revenue_output \
    --bootstrap-server broker-1:9092 \
    --partitions 3 \
    --replication-factor 3
```
Purpose: create topics required by producer, Flink job, and sinks.

---

### Step 7 — Deploy Kafka Connect connectors (Mongo + optional Snowflake)
Mongo deploy with idempotency check:
```bash
EXISTING_CONNECTORS=$(curl -s "http://localhost:8083/connectors")
if [[ $EXISTING_CONNECTORS == *"mongo-sink"* ]]; then
    echo -e " ${YELLOW}Already exists (Skipping)${NC}"
else
    response=$(curl -s -X POST -H "Content-Type: application/json" --data @connect/mongo-sink.json http://localhost:8083/connectors)
    if [[ $response == *"error_code"* ]]; then
        echo -e " ${RED}Failed!${NC} $response"
    else
        echo -e " ${GREEN}Success!${NC}"
    fi
fi
```
Snowflake conditional deploy:
```bash
if grep -q "YOUR_SNOWFLAKE_URL" connect/snowflake-sink.json; then
    echo -e "${YELLOW}Skipping Snowflake (Credentials not set in connect/snowflake-sink.json)${NC}"
else
    curl -s -X POST -H "Content-Type: application/json" --data @connect/snowflake-sink.json http://localhost:8083/connectors > /dev/null
fi
```
Purpose: deploy Sink connectors for Mongo and Snowflake; Snowflake is skipped when placeholders are present.

---

### Step 8 — Submit Flink SQL job
```bash
docker exec -t jobmanager ./bin/sql-client.sh -f /opt/flink/sql/store_aggregation.sql
```
Purpose: Create Flink SQL Task that aggregates sales and writes output to `store_revenue_output`.

---

### Final output & data generation hint
```bash
echo -e "${GREEN}Services Available:${NC}"
echo -e "  🔹 Kafka UI:        http://localhost:8080"
echo -e "  🔹 Schema Registry: http://localhost:8081 (internal use only)"
echo -e "  🔹 Kafka Connect:   http://localhost:8083 (internal use only)"
echo -e "  🔹 Sales API Docs:  http://localhost:8000/docs"
echo -e "  🔹 Mongo Express:   http://localhost:8084"
echo -e "  🔹 Flink Dashboard: http://localhost:8085"

echo -e "To generate 100 new sale events, run:"
echo -e "python testing/load-generator.py --count 100"
```

---

## Services Overview

| Service          | URL                              | Usage                                   |
|------------------|----------------------------------|-----------------------------------------|
| Kafka UI         | http://localhost:8080           | View Topics, Messages, and Partitions   |
| Schema Registry  | http://localhost:8081           | (Inside Kafka UI) View Avro Schemas     |
| Kafka Connect    | http://localhost:8083           | Manage connectors for sinks             |
| Mongo Express    | http://localhost:8084           | View Sink Data in MongoDB               |
| Flink Dashboard  | http://localhost:8085           | Monitor Streaming Jobs                  |
| Sales API Docs   | http://localhost:8000/docs      | Swagger UI for the Producer             |

---

## Quick troubleshooting & useful commands

- Inspect logs:
  - docker logs broker-1
  - docker logs kafka-connect
  - docker logs sales-api

- Clean teardown (if cluster ID mismatch or you need a fresh start):
```bash
docker-compose down -v
docker volume prune -f
```

- Rebuild only sales-api after code changes:

```bash
docker-compose up -d --build --no-deps sales-api
```

---

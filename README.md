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
│   ├── test-application/
│   │   └── main.py               # PyFlink DataStream job: uppercase topic-to-topic relay
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
  - **PyFlink Uppercase Job:** A detached Python DataStream job that reads from `poc_raw_sale_events` and writes uppercase strings to `poc_transformed_sale_events`.

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

## Setup lifecycle (what setup.sh does)

`setup.sh` now follows a deterministic production-like deployment order:

1. Starts/rebuilds infrastructure with Docker Compose.
2. Waits for core services to be healthy (Kafka UI, Schema Registry, Kafka Connect, Mongo Express, Flink UI).
3. Deploys Avro schema and restarts Sales API.
4. Creates required Kafka topics (including `poc_raw_sale_events` and `poc_transformed_sale_events`).
5. Deploys connectors idempotently:
  - Mongo connector always checked/deployed.
  - Snowflake connector deployed only when `ENABLE_SNOWFLAKE_CONNECTOR=true` and credentials are present.
6. Submits Flink SQL job from `flink/sql/store_aggregation.sql`.
7. Submits PyFlink uppercase job in detached mode from `flink/test-application/main.py`.

SQL and PyFlink jobs are both kept and run together on every setup. Ordering is fixed: SQL submission first, then PyFlink submission.

### Idempotent reruns

- Connector creation is rerun-safe: existing connectors are detected and skipped.
- PyFlink submission is rerun-safe: existing running job is skipped by default.
- To force redeploy the PyFlink job on setup reruns:

```bash
FORCE_PYFLINK_REDEPLOY=true ./setup.sh
```

- To enable Snowflake connector deployment (still gated by credential presence in connector config):

```bash
ENABLE_SNOWFLAKE_CONNECTOR=true ./setup.sh
```

The following section breaks down the file so users can also understand each step of the following 

## Setup.sh — step-by-step mapping (snippets and explanation)

Note: the code snippets are taken from setup.sh and grouped by the script's logical steps so you can compare easily.

### Step 0 - Setting up helper function to check service status: wait_for_service
```bash
wait_for_service() {
    local url=$1
    local name=$2
  local max_retries=${3:-30}
    local count=0
    echo -n "Waiting for $name..."
  until curl --output /dev/null --silent --show-error --fail "$url"; do
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
if get_connectors | grep -q '"mongo-sink-sales"'; then
    echo -e " ${YELLOW}Already exists (Skipping)${NC}"
else
  response=$(curl --silent --show-error --fail -X POST -H "Content-Type: application/json" --data @connect/mongo-sink.json http://localhost:8083/connectors)
    if [[ $response == *"error_code"* ]]; then
        echo -e " ${RED}Failed!${NC} $response"
    else
        echo -e " ${GREEN}Success!${NC}"
    fi
fi
```
Snowflake conditional deploy:
```bash
if [[ "${ENABLE_SNOWFLAKE_CONNECTOR}" != "true" ]]; then
  echo -e "${YELLOW}Skipping Snowflake (set ENABLE_SNOWFLAKE_CONNECTOR=true to enable)${NC}"
elif ! snowflake_credentials_present; then
  echo -e "${YELLOW}Skipping Snowflake (missing snowflake.private.key)${NC}"
else
  deploy_connector_if_missing "kafka-sink-sales2" "connect/snowflake-sink.json"
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

### Step 9 — Submit PyFlink uppercase job (detached)
```bash
docker exec -t jobmanager env \
  PYFLINK_CLIENT_EXECUTABLE=python3 \
  PYTHONPATH=/opt/flink/opt/python:/opt/flink/opt/python/pyflink:/opt/flink/opt/python/py4j-0.10.9.7-src.zip:/opt/flink/opt/python/cloudpickle-2.2.0-src.zip \
  ./bin/flink run -d -py /opt/flink/test-application/main.py
```
Purpose: deploy the long-running Python Flink job that consumes plain strings from `poc_raw_sale_events`, uppercases them, and publishes to `poc_transformed_sale_events`.

## Verify both Flink jobs independently

List all running Flink jobs:

```bash
docker exec -t jobmanager ./bin/flink list --running
```

Verify the SQL job is running:

```bash
docker exec -t jobmanager ./bin/flink list --running | grep -i "store_aggregation"
```

Verify the PyFlink uppercase job is running:

```bash
docker exec -t jobmanager ./bin/flink list --running | grep -F "Kafka to Kafka Uppercase Application"
```

Verify POC topics exist:

```bash
docker exec -t broker-1 kafka-topics --bootstrap-server broker-1:9092 --list | grep -E "poc_raw_sale_events|poc_transformed_sale_events"
```

Produce test records and validate uppercase flow end-to-end:

```bash
docker exec -it broker-1 kafka-console-producer --bootstrap-server broker-1:9092 --topic poc_raw_sale_events
```

In a second terminal:

```bash
docker exec -it broker-1 kafka-console-consumer --bootstrap-server broker-1:9092 --topic poc_transformed_sale_events --from-beginning
```

Type lowercase messages in producer; consumer output should be uppercase.

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

- Check running Flink jobs:
```bash
docker exec -t jobmanager ./bin/flink list --running
```

- Test the uppercase pipeline end-to-end:
```bash
docker exec -it broker-1 kafka-console-producer --bootstrap-server broker-1:9092 --topic poc_raw_sale_events
docker exec -it broker-1 kafka-console-consumer --bootstrap-server broker-1:9092 --topic poc_transformed_sale_events --from-beginning
```

- Run one-command smoke test (produce, verify uppercase output, and validate job remains running):
```bash
chmod +x scripts/smoke_test_uppercase.sh
./scripts/smoke_test_uppercase.sh
```

---

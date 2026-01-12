#!/bin/bash

# ==============================================================================
# Kafka Workshop 2 - Environment Setup & Verification Script
# ==============================================================================

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Starting Workshop Environment Setup...${NC}"

# 1. Check Docker
if ! docker info > /dev/null 2>&1; then
  echo -e "${RED}Error: Docker is not running.${NC}"
  exit 1
fi

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

# 2. Verify Infrastructure Health
echo -e "\n${YELLOW}[Step 1/5] Verifying Service Health...${NC}"
wait_for_service "http://localhost:8081" "Schema Registry"
wait_for_service "http://localhost:8083" "Kafka Connect"
wait_for_service "http://localhost:8080" "Kafka UI"

# 3. Deploy Schema (The CI/CD Simulation)
echo -e "\n${YELLOW}[Step 2/5] Deploying Schema to Registry...${NC}"
# We use 'run --rm' so this works even if the API container is currently crashed
docker-compose run --rm sales-api python /app/deploy_schema.py

echo -e "${YELLOW}Restarting Sales API to pick up new schema...${NC}"
docker-compose restart sales-api
# Wait for the API to actually come up this time
wait_for_service "http://localhost:8000/docs" "Sales API"

# 4. Create Topics
echo -e "\n${YELLOW}[Step 3/5] Creating Topics...${NC}"
docker exec broker-1 kafka-topics --create --if-not-exists \
    --topic raw_sale_events_topic \
    --bootstrap-server broker-1:9092 \
    --partitions 3 \
    --replication-factor 3
echo -e "${GREEN}Topic 'raw_sale_events_topic' confirmed.${NC}"

# 5. Create Flink Output Topic
docker exec broker-1 kafka-topics --create --if-not-exists \
    --topic store_revenue_output \
    --bootstrap-server broker-1:9092 \
    --partitions 1 \
    --replication-factor 1

# 6. Configure Connectors
echo -e "\n${YELLOW}[Step 4/5] Deploying Kafka Connectors...${NC}"

# Mongo
echo -n "Deploying Mongo Sink..."
response=$(curl -s -X POST -H "Content-Type: application/json" --data @connect/mongo-sink.json http://localhost:8083/connectors)
if [[ $response == *"error_code"* ]]; then
    echo -e " ${RED}Failed!${NC} $response"
else
    echo -e " ${GREEN}Success!${NC}"
fi

# Snowflake (Optional)
if grep -q "YOUR_SNOWFLAKE_URL" connect/snowflake-sink.json; then
    echo -e "${YELLOW}Skipping Snowflake (Credentials not set)${NC}"
else
    echo -n "Deploying Snowflake Sink..."
    curl -s -X POST -H "Content-Type: application/json" --data @connect/snowflake-sink.json http://localhost:8083/connectors > /dev/null
    echo -e " ${GREEN}Sent!${NC}"
fi

echo -e "\n${GREEN}====================================================${NC}"
echo -e "${GREEN}      Workshop Environment Ready!                   ${NC}"
echo -e "${GREEN}====================================================${NC}"
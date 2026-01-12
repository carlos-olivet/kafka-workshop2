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

# 1. Check if Docker is running
if ! docker info > /dev/null 2>&1; then
  echo -e "${RED}Error: Docker is not running. Please start Docker Desktop/Daemon.${NC}"
  exit 1
fi

# 2. Spin up the infrastructure
echo -e "\n${YELLOW}[Step 1/4] Starting Docker Containers (this may take a moment)...${NC}"
docker-compose up -d --build

# Function to wait for a service to be ready
wait_for_service() {
    local url=$1
    local name=$2
    local max_retries=30
    local count=0

    echo -n "Waiting for $name to be ready..."
    until curl --output /dev/null --silent --head --fail "$url"; do
        printf '.'
        count=$((count+1))
        if [ $count -ge $max_retries ]; then
            echo -e "\n${RED}Timeout waiting for $name! Check docker logs.${NC}"
            return 1
        fi
        sleep 5
    done
    echo -e " ${GREEN}OK!${NC}"
}

# 3. Health Checks
echo -e "\n${YELLOW}[Step 2/4] Verifying Service Health...${NC}"
wait_for_service "http://localhost:8081" "Schema Registry"
wait_for_service "http://localhost:8083" "Kafka Connect"
wait_for_service "http://localhost:8080" "Kafka UI"
wait_for_service "http://localhost:8000/docs" "Sales API"

# 4. Verify Topics
echo -e "\n${YELLOW}[Step 3/4] Verifying Topic Creation...${NC}"
# We assume the API or Connect auto-creates topics, but we can force create the main one just in case
docker exec broker-1 kafka-topics --create --if-not-exists --topic raw_sale_events_topic --bootstrap-server broker-1:9092 --partitions 3 --replication-factor 3
echo -e "${GREEN}Topic 'raw_sale_events_topic' confirmed.${NC}"

echo -e "\n${GREEN}====================================================${NC}"
echo -e "${GREEN}      Workshop Environment Ready!                   ${NC}"
echo -e "${GREEN}====================================================${NC}"
echo -e "Kafka UI:      http://localhost:8080"
echo -e "Sales API:     http://localhost:8000/docs"
echo -e "Flink UI:      http://localhost:8081"
echo -e "Connect REST:  http://localhost:8083/connectors"
echo -e "\nRun 'python testing/load_generator.py --count 10' to test traffic."

# 4. CI/CD Schema Deployment
echo -e "\n${YELLOW}[Step 2/3] Deploying Schemas via CI/CD Script...${NC}"
# We execute the script INSIDE the container to reuse its network and python libs
docker exec sales-api python /app/deploy_schema.py

# 5. Restart API to pick up the schema
# (Since the API might have failed to init producer before schema existed)
echo -e "${YELLOW}Restarting Sales API to load new schema...${NC}"
docker-compose restart sales-api
# Wait for it to come back
sleep 5

# 6. Configure Kafka Connectors
echo -e "\n${YELLOW}[Step 4/4] Deploying Kafka Connectors...${NC}"

# Deploy Mongo Sink
echo -n "Deploying Mongo Sink..."
response=$(curl -s -X POST -H "Content-Type: application/json" --data @connect/mongo-sink.json http://localhost:8083/connectors)
if [[ $response == *"error_code"* ]]; then
    echo -e " ${RED}Failed!${NC} Response: $response"
else
    echo -e " ${GREEN}Success!${NC}"
fi

# Deploy Snowflake Sink (Example placeholder - expects user to fill credentials in JSON first)
# We warn the user if they haven't updated the config file yet.
if grep -q "YOUR_SNOWFLAKE_URL" connect/snowflake-sink.json; then
    echo -e "${YELLOW}Skipping Snowflake Sink: Credentials not set in connect/snowflake-sink.json${NC}"
else
    echo -n "Deploying Snowflake Sink..."
    response=$(curl -s -X POST -H "Content-Type: application/json" --data @connect/snowflake-sink.json http://localhost:8083/connectors)
    if [[ $response == *"error_code"* ]]; then
        echo -e " ${RED}Failed!${NC} Response: $response"
    else
        echo -e " ${GREEN}Success!${NC}"
    fi
fi


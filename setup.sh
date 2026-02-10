#!/bin/bash

# ==============================================================================
# Kafka Workshop 2 - Environment Setup & Verification Script
# ==============================================================================

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check service availability
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

echo -e "\n${GREEN}====================================================${NC}"
echo -e "${YELLOW}Starting Workshop Environment Setup...${NC}"
echo -e "\n${GREEN}====================================================${NC}"

# Step 1: Check Docker
if ! docker info > /dev/null 2>&1; then
  echo -e "${RED}Error: Docker is not running.${NC}"
  exit 1
fi


# Step 2: Build the Docker images (forcing a rebuild to ensure latest changes)
echo -e "${YELLOW}Building Docker images...${NC}"
docker-compose build

# Step 3: Start the containers (if they aren't already running)
echo -e "${YELLOW}Starting containers...${NC}"
docker-compose up -d

wait_for_service() {
    local url=$1
    local name=$2
    local max_retries=30
    local count=0
    echo -n "Waiting for $name..."
    until curl --output /dev/null --silent --get --fail "$url"; do
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

# 2. Verify Infrastructure Health - Part 1
echo -e "\n${YELLOW}[Step 1/7] Verifying Service Health...${NC}"
wait_for_service "http://localhost:8080" "Kafka UI"
wait_for_service "http://localhost:8081" "Schema Registry"
wait_for_service "http://localhost:8083" "Kafka Connect"
wait_for_service "http://localhost:8084" "Mongo Express"
wait_for_service "http://localhost:8085" "Flink Dashboard"


# 3. Deploy Schema (The CI/CD Simulation)
echo -e "\n${YELLOW}[Step 2/7] Deploying Schema to Registry...${NC}"
# We use 'run --rm' so this works even if the API container is currently crashed
docker-compose run --rm sales-api python /app/deploy_schema.py

echo -e "${YELLOW}[Step 3/7] Restarting Sales API to pick up new schema...${NC}"
docker-compose restart sales-api
# Wait for the API to actually come up this time
wait_for_service "http://localhost:8000/docs" "Sales API"

# 4. Create Topics
echo -e "\n${YELLOW}[Step 4/7] Creating Topics...${NC}"
# 4.1 Create Raw Sales Events Topic
docker exec broker-1 kafka-topics --create --if-not-exists \
    --topic raw_sale_events_topic \
    --bootstrap-server broker-1:9092 \
    --partitions 3 \
    --replication-factor 3
echo -e "${GREEN}Topic 'raw_sale_events_topic' confirmed.${NC}"

# 4.2 Create Flink Output Topic
docker exec broker-1 kafka-topics --create --if-not-exists \
    --topic store_revenue_output \
    --bootstrap-server broker-1:9092 \
    --partitions 3 \
    --replication-factor 3
echo -e "${GREEN}Topic 'store_revenue_output' confirmed.${NC}"

# 5. Configure Connectors
echo -e "\n${YELLOW}[Step 5/7] Deploying Kafka Connectors...${NC}"

# 5.1 Deploy Mongo Connector
echo -n "Deploying Mongo Sink..."
# Check if connector already exists to avoid error
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

# 5.2 Deploy Snowflake Connector Snowflake (Optional as it requires Snowflake account and crednetials)
if grep -q "YOUR_SNOWFLAKE_URL" connect/snowflake-sink.json; then
    echo -e "${YELLOW}Skipping Snowflake (Credentials not set in connect/snowflake-sink.json)${NC}"
else
    echo -n "Deploying Snowflake Sink..."
    # Check if snowflake connector exists
    if [[ $EXISTING_CONNECTORS == *"snowflake-sink"* ]]; then
        echo -e " ${YELLOW}Already exists (Skipping)${NC}"
    else
        curl -s -X POST -H "Content-Type: application/json" --data @connect/snowflake-sink.json http://localhost:8083/connectors > /dev/null
        echo -e " ${GREEN}Sent!${NC}"
    fi
fi

# 6. Create Flink Job to process store averages
echo -e "\n${YELLOW}[Step 6/7] Create a Flink Job to process Data${NC}"
docker exec -t jobmanager ./bin/sql-client.sh -f /opt/flink/sql/store_aggregation.sql

echo -e "\n${GREEN}====================================================${NC}"
echo -e "${GREEN}      Workshop Environment Ready!                   ${NC}"
echo -e "${GREEN}====================================================${NC}"

echo -e "\n${YELLOWDOCK}[Step 7/7] Generate Data for the topics${NC}"
echo -e "To generate 100 new sale events, run the following command in your terminal:"
echo -e "\n${GREEN}====================================================${NC}"
echo -e "${GREEN}python testing/load-generator.py --count 100${NC}"
echo -e "\n${GREEN}====================================================${NC}"


echo -e "\n${YELLOW}Services Available:${NC}"
echo -e "  🔹 ${YELLOW}Kafka UI:${NC}        http://localhost:8080"
echo -e "  🔹 ${YELLOW}Schema Registry:${NC} http://localhost:8081 (internal use only)" 
echo -e "  🔹 ${YELLOW}Kafka Connect:${NC}   http://localhost:8083 (internal use only)"
echo -e "  🔹 ${YELLOW}Sales API Docs:${NC}  http://localhost:8000/docs"
echo -e "  🔹 ${YELLOW}Mongo Express:${NC}   http://localhost:8084 (admin & pass)"
echo -e "  🔹 ${YELLOW}Flink Dashboard:${NC} http://localhost:8085"





#!/bin/bash

set -euo pipefail

# ==============================================================================
# Kafka Workshop 2 - Environment Setup & Verification Script
# ==============================================================================

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

PYFLINK_JOB_NAME="Kafka to Kafka Uppercase Application"
ENABLE_SNOWFLAKE_CONNECTOR="${ENABLE_SNOWFLAKE_CONNECTOR:-false}"
FORCE_PYFLINK_REDEPLOY="${FORCE_PYFLINK_REDEPLOY:-false}"
MONGO_CONNECTOR_CONFIG="connect/mongo-sink.json"
SNOWFLAKE_CONNECTOR_CONFIG="connect/snowflake-sink.json"

extract_connector_name() {
    local config_path=$1
    sed -nE 's/^[[:space:]]*"name"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/p' "${config_path}" | head -n1
}

MONGO_CONNECTOR_NAME="$(extract_connector_name "${MONGO_CONNECTOR_CONFIG}")"
SNOWFLAKE_CONNECTOR_NAME="$(extract_connector_name "${SNOWFLAKE_CONNECTOR_CONFIG}")"

if [[ -z "${MONGO_CONNECTOR_NAME}" ]]; then
    echo -e "${RED}Failed to read connector name from ${MONGO_CONNECTOR_CONFIG}${NC}"
    exit 1
fi

if [[ -z "${SNOWFLAKE_CONNECTOR_NAME}" ]]; then
    echo -e "${RED}Failed to read connector name from ${SNOWFLAKE_CONNECTOR_CONFIG}${NC}"
    exit 1
fi

# Function to check service availability
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

run_critical_step() {
    local description=$1
    shift

    echo -e "${YELLOW}${description}...${NC}"
    "$@"
    echo -e "${GREEN}${description} completed.${NC}"
}

get_connectors() {
    curl --silent --show-error --fail "http://localhost:8083/connectors"
}

deploy_connector_if_missing() {
    local connector_name=$1
    local config_path=$2

    echo -n "Deploying ${connector_name}..."
    if get_connectors | grep -q "\"${connector_name}\""; then
        echo -e " ${YELLOW}Already exists (Skipping)${NC}"
        return 0
    fi

    local response
    response=$(curl --silent --show-error --fail -X POST -H "Content-Type: application/json" --data @"${config_path}" http://localhost:8083/connectors)
    if [[ $response == *"error_code"* ]]; then
        echo -e " ${RED}Failed!${NC} $response"
        return 1
    fi

    echo -e " ${GREEN}Success!${NC}"
}

get_pyflink_job_id() {
    docker exec jobmanager ./bin/flink list --running 2>/dev/null | awk -v target="${PYFLINK_JOB_NAME}" '$0 ~ target {print $4; exit}'
}

is_pyflink_job_running() {
    [[ -n "$(get_pyflink_job_id)" ]]
}

deploy_pyflink_job() {
    local running_job_id
    running_job_id="$(get_pyflink_job_id)"

    if [[ -n "${running_job_id}" ]]; then
        if [[ "${FORCE_PYFLINK_REDEPLOY}" == "true" ]]; then
            echo -e "${YELLOW}FORCE_PYFLINK_REDEPLOY=true, cancelling existing PyFlink job ${running_job_id}.${NC}"
            docker exec -t jobmanager ./bin/flink cancel "${running_job_id}"
        else
            echo -e "${YELLOW}PyFlink job already running (Skipping). Set FORCE_PYFLINK_REDEPLOY=true to redeploy.${NC}"
            return 0
        fi
    fi

    docker exec -t jobmanager env \
        PYFLINK_CLIENT_EXECUTABLE=python3 \
        PYTHONPATH=/opt/flink/opt/python:/opt/flink/opt/python/pyflink:/opt/flink/opt/python/py4j-0.10.9.7-src.zip:/opt/flink/opt/python/cloudpickle-2.2.0-src.zip \
        ./bin/flink run -d -py /opt/flink/test-application/main.py
    echo -e "${GREEN}PyFlink uppercase job submitted.${NC}"
}

snowflake_credentials_present() {
    local private_key
    private_key=$(sed -nE 's/^[[:space:]]*"snowflake\.private\.key"[[:space:]]*:[[:space:]]*"(.*)".*/\1/p' "${SNOWFLAKE_CONNECTOR_CONFIG}" | head -n1)
    [[ -n "${private_key}" ]]
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
run_critical_step "Building Docker images" docker-compose build

# Step 3: Start the containers (if they aren't already running)
run_critical_step "Starting containers" docker-compose up -d

# 2. Verify Infrastructure Health - Part 1
echo -e "\n${YELLOW}[Step 1/8] Verifying Service Health...${NC}"
wait_for_service "http://localhost:8080" "Kafka UI"
wait_for_service "http://localhost:8081" "Schema Registry"
wait_for_service "http://localhost:8083" "Kafka Connect"
wait_for_service "http://localhost:8084" "Mongo Express"
wait_for_service "http://localhost:8085/overview" "Flink Dashboard" 60


# 3. Deploy Schema (The CI/CD Simulation)
echo -e "\n${YELLOW}[Step 2/8] Deploying Schema to Registry...${NC}"
# We use 'run --rm' so this works even if the API container is currently crashed
run_critical_step "Deploying schema" docker-compose run --rm sales-api python /app/deploy_schema.py

echo -e "${YELLOW}[Step 3/8] Restarting Sales API to pick up new schema...${NC}"
run_critical_step "Restarting Sales API" docker-compose restart sales-api
# Wait for the API to actually come up this time
wait_for_service "http://localhost:8000/docs" "Sales API"

# 4. Create Topics
echo -e "\n${YELLOW}[Step 4/8] Creating Topics...${NC}"
# 4.1 Create Raw Sales Events Topic
run_critical_step "Creating topic raw_sale_events_topic" docker exec broker-1 kafka-topics --create --if-not-exists \
    --topic raw_sale_events_topic \
    --bootstrap-server broker-1:9092 \
    --partitions 3 \
    --replication-factor 3
echo -e "${GREEN}Topic 'raw_sale_events_topic' confirmed.${NC}"

run_critical_step "Creating topic poc_raw_sale_events" docker exec broker-1 kafka-topics --create --if-not-exists \
    --topic poc_raw_sale_events \
    --bootstrap-server broker-1:9092 \
    --partitions 3 \
    --replication-factor 3
echo -e "${GREEN}Topic 'poc_raw_sale_events' confirmed.${NC}"

run_critical_step "Creating topic poc_transformed_sale_events" docker exec broker-1 kafka-topics --create --if-not-exists \
    --topic poc_transformed_sale_events \
    --bootstrap-server broker-1:9092 \
    --partitions 3 \
    --replication-factor 3
echo -e "${GREEN}Topic 'poc_transformed_sale_events' confirmed.${NC}"


# 4.2 Create Flink Output Topic
run_critical_step "Creating topic store_revenue_output" docker exec broker-1 kafka-topics --create --if-not-exists \
    --topic store_revenue_output \
    --bootstrap-server broker-1:9092 \
    --partitions 3 \
    --replication-factor 3
echo -e "${GREEN}Topic 'store_revenue_output' confirmed.${NC}"

# 5. Configure Connectors
echo -e "\n${YELLOW}[Step 5/8] Deploying Kafka Connectors...${NC}"

# 5.1 Deploy Mongo Connector
deploy_connector_if_missing "${MONGO_CONNECTOR_NAME}" "${MONGO_CONNECTOR_CONFIG}"

# 5.2 Deploy Snowflake Connector (Optional)
if [[ "${ENABLE_SNOWFLAKE_CONNECTOR}" != "true" ]]; then
    echo -e "${YELLOW}Skipping Snowflake (set ENABLE_SNOWFLAKE_CONNECTOR=true to enable)${NC}"
elif ! snowflake_credentials_present; then
    echo -e "${YELLOW}Skipping Snowflake (missing snowflake.private.key in ${SNOWFLAKE_CONNECTOR_CONFIG})${NC}"
else
    deploy_connector_if_missing "${SNOWFLAKE_CONNECTOR_NAME}" "${SNOWFLAKE_CONNECTOR_CONFIG}"
fi

# 6. Create Flink SQL job to process store averages
echo -e "\n${YELLOW}[Step 6/8] Submitting Flink SQL job...${NC}"
run_critical_step "Submitting Flink SQL job" docker exec -t jobmanager ./bin/sql-client.sh -f /opt/flink/sql/store_aggregation.sql

# 7. Deploy PyFlink uppercase job in detached mode
echo -e "\n${YELLOW}[Step 7/8] Deploying PyFlink uppercase job...${NC}"
run_critical_step "Submitting PyFlink uppercase job" deploy_pyflink_job

echo -e "\n${GREEN}====================================================${NC}"
echo -e "${GREEN}      Workshop Environment Ready!                   ${NC}"
echo -e "${GREEN}====================================================${NC}"

echo -e "\n${YELLOW}[Step 8/8] Generate Data for the topics${NC}"
echo -e "To generate 100 new sale events, run the following command in your terminal:"
echo -e "\n${GREEN}====================================================${NC}"
echo -e "${GREEN}python testing/load-generator.py --count 100${NC}"
echo -e "\n${GREEN}====================================================${NC}"
echo -e "For uppercase demo messages (POC topics), run:"
echo -e "${GREEN}docker exec -it broker-1 kafka-console-producer --bootstrap-server broker-1:9092 --topic poc_raw_sale_events${NC}"
echo -e "${GREEN}docker exec -it broker-1 kafka-console-consumer --bootstrap-server broker-1:9092 --topic poc_transformed_sale_events --from-beginning${NC}"


echo -e "\n${YELLOW}Services Available:${NC}"
echo -e "  🔹 ${YELLOW}Kafka UI:${NC}        http://localhost:8080"
echo -e "  🔹 ${YELLOW}Schema Registry:${NC} http://localhost:8081 (internal use only)" 
echo -e "  🔹 ${YELLOW}Kafka Connect:${NC}   http://localhost:8083 (internal use only)"
echo -e "  🔹 ${YELLOW}Sales API Docs:${NC}  http://localhost:8000/docs"
echo -e "  🔹 ${YELLOW}Mongo Express:${NC}   http://localhost:8084 (admin & pass)"
echo -e "  🔹 ${YELLOW}Flink Dashboard:${NC} http://localhost:8085"





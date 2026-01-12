import sys
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.error import SchemaRegistryError

# Configuration
SCHEMA_REGISTRY_URL = "http://schema-registry:8081" # Internal Docker URL
SUBJECT_NAME = "raw_sale_events_topic-value"
SCHEMA_FILE = "/app/purchase_event.avsc" 

def deploy_schema():
    client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
    
    print(f"--- Deploying Schema for Subject: {SUBJECT_NAME} ---")

    try:
        with open(SCHEMA_FILE, 'r') as f:
            schema_str = f.read()
    except FileNotFoundError:
        print(f"Error: Could not find schema file at {SCHEMA_FILE}")
        sys.exit(1)
    
    schema = Schema(schema_str, schema_type="AVRO")

    try:
        # Check if this exact schema version already exists
        registered = client.lookup_schema(SUBJECT_NAME, schema)
        print(f"✅ Schema already registered. ID: {registered.schema_id}, Version: {registered.version}")
    except SchemaRegistryError as e:
        if e.error_code in [40401, 40403]: # Not found
            print("Schema not found. Registering new version...")
            schema_id = client.register_schema(SUBJECT_NAME, schema)
            print(f"🚀 Successfully registered schema ID: {schema_id}")
        else:
            print(f"❌ Registry Error: {e}")
            sys.exit(1)

if __name__ == "__main__":
    deploy_schema()
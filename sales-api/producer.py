from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

class KafkaSalesProducer:
    def __init__(self, bootstrap_servers, schema_registry_url):
        
        # 1. Connect to Registry
        schema_registry_conf = {'url': schema_registry_url}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        # 2. Fetch Latest Schema from Registry (The "Source of Truth")
        subject_name = "raw_sale_events_topic-value"
        try:
            # Get latest version
            registered_schema = schema_registry_client.get_latest_version(subject_name)
            schema_str = registered_schema.schema.schema_str
            print(f"✅ Fetched Schema ID {registered_schema.schema_id} from Registry")
        except Exception as e:
            print(f"❌ Failed to fetch schema for {subject_name}. Is it registered?")
            raise e

        # 3. Configure Serializer
        avro_serializer = AvroSerializer(
            schema_registry_client,
            schema_str,
            lambda obj, ctx: obj
        )

        producer_conf = {
            'bootstrap.servers': bootstrap_servers,
            'key.serializer': avro_serializer,
            'value.serializer': avro_serializer
        }
        self.producer = SerializingProducer(producer_conf)

    async def send_event(self, topic, event_dict):
        def delivery_report(err, msg):
            if err is not None:
                print(f"Delivery failed: {err}")
        
        self.producer.produce(topic=topic, value=event_dict, on_delivery=delivery_report)
        self.producer.poll(0)
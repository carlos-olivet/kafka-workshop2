import sys
import os
from pyflink.common.typeinfo import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, 
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee
)

JOB_NAME = "Kafka to Kafka Uppercase Application"


def get_env_int(name, default_value):
    value = os.getenv(name)
    if value is None or value == "":
        return default_value
    try:
        return int(value)
    except ValueError:
        print(f"Invalid integer for {name}: {value}. Using default={default_value}")
        return default_value


def uppercase_and_log(msg):
    return msg.upper()

def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker-1:9092")
    source_topic = os.getenv("KAFKA_SOURCE_TOPIC", "poc_raw_sale_events")
    sink_topic = os.getenv("KAFKA_SINK_TOPIC", "poc_transformed_sale_events")
    consumer_group = os.getenv("KAFKA_CONSUMER_GROUP", "uppercase-consumer-group")
    startup_mode = os.getenv("KAFKA_STARTUP_MODE", "earliest").lower()
    checkpoint_interval_ms = get_env_int("CHECKPOINT_INTERVAL_MS", 60000)
    parallelism = get_env_int("FLINK_PARALLELISM", 1)
    job_name = os.getenv("FLINK_JOB_NAME", JOB_NAME)

    # 1. Setup the Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_python_executable("python3")
    env.set_parallelism(parallelism)
    if checkpoint_interval_ms > 0:
        env.enable_checkpointing(checkpoint_interval_ms)

    if startup_mode == "latest":
        starting_offsets = KafkaOffsetsInitializer.latest()
    else:
        starting_offsets = KafkaOffsetsInitializer.earliest()

    # 2. Define the Kafka Source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(bootstrap_servers) \
        .set_topics(source_topic) \
        .set_group_id(consumer_group) \
        .set_starting_offsets(starting_offsets) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # 3. Create the DataStream
    data_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="Raw Sales Kafka Source"
    )

    # 4. Apply the Transformation
    uppercase_stream = data_stream.map(
        uppercase_and_log, 
        output_type=Types.STRING()
    )

    # 5. Define the Kafka Sink
    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers(bootstrap_servers) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(sink_topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    # 6. Attach Sink & Execute
    uppercase_stream.sink_to(kafka_sink)

    print(f"Starting detached PyFlink job: {job_name}")
    print(f"Kafka bootstrap: {bootstrap_servers}")
    print(f"Source topic: {source_topic} | Sink topic: {sink_topic}")
    sys.stdout.flush()
    try:
        # execute() is a blocking call. It will run forever until interrupted.
        env.execute(job_name)
    except KeyboardInterrupt:
        print("Received shutdown signal.")
        sys.exit(0)
    except Exception as e:
        print(f"Application crashed with error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()

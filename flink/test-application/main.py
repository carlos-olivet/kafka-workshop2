import sys
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


def uppercase_and_log(msg):
    return msg.upper()

def main():
    # Kafka connector jars are already added to /opt/flink/lib in the Flink image.
    # This app is submitted from the jobmanager container by setup.sh.
    # 1. Setup the Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_python_executable("python3")
    env.set_parallelism(1)

    # 2. Define the Kafka Source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("broker-1:9092") \
        .set_topics("poc_raw_sale_events") \
        .set_group_id("uppercase-consumer-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
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
        .set_bootstrap_servers("broker-1:9092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("poc_transformed_sale_events")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    # 6. Attach Sink & Execute
    uppercase_stream.sink_to(kafka_sink)

    print(f"Starting detached PyFlink job: {JOB_NAME}")
    sys.stdout.flush()
    try:
        # execute() is a blocking call. It will run forever until interrupted.
        env.execute(JOB_NAME)
    except KeyboardInterrupt:
        print("Received shutdown signal.")
        sys.exit(0)
    except Exception as e:
        print(f"Application crashed with error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()

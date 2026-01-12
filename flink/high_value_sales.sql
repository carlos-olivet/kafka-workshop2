-- 1. Create Source Table from Kafka
CREATE TABLE raw_sales (
    transaction_id STRING,
    user_id STRING,
    total_amount FLOAT,
    `timestamp` STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'raw_sale_events_topic',
    'properties.bootstrap.servers' = 'broker-1:9092',
    'properties.group.id' = 'flink-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://schema-registry:8081'
);

-- 2. Processing Query: Filter High Value Sales > 100
SELECT 
    user_id, 
    COUNT(transaction_id) as sales_count, 
    SUM(total_amount) as total_spent
FROM raw_sales
WHERE total_amount > 100
GROUP BY user_id;
-- 1. Define the Source (Reading from Kafka)
CREATE TABLE raw_sales (
    store_id STRING,
    total_amount FLOAT,
    `timestamp` STRING,
    WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'raw_sale_events_topic',
    'properties.bootstrap.servers' = 'broker-1:29092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://schema-registry:8081'
);

-- 2. Define the Sink (Writing back to Kafka for the Dashboard)
CREATE TABLE store_revenue_stream (
    store_id STRING,
    window_end TIMESTAMP(3),
    total_revenue FLOAT
) WITH (
    'connector' = 'kafka',
    'topic' = 'store_revenue_output',
    'properties.bootstrap.servers' = 'broker-1:29092',
    'format' = 'json' -- We use JSON here for easy reading in UI
);

-- 3. The Logic (Tumbling Window)
INSERT INTO store_revenue_stream
SELECT 
    store_id,
    TUMBLE_END(`timestamp`, INTERVAL '1' MINUTE) as window_end,
    SUM(total_amount) as total_revenue
FROM raw_sales
GROUP BY 
    store_id, 
    TUMBLE(`timestamp`, INTERVAL '1' MINUTE);
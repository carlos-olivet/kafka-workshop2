-- 0. Set tables to take 5 seconds of no data as enough to close watermark
SET 'table.exec.source.idle-timeout' = '5000 ms';


-- 1. Define the Source (Reading from Kafka)
CREATE TABLE raw_sales (
    store_id STRING,
    total_amount FLOAT,
    `timestamp` STRING,
    row_time as to_timestamp(`timestamp`),
    WATERMARK FOR `row_time` AS `row_time` - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'raw_sale_events_topic',
    'properties.bootstrap.servers' = 'broker-1:9092',
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
    'properties.bootstrap.servers' = 'broker-1:9092',
    'format' = 'json' -- We use JSON here for easy reading in UI
);

-- 3. The Logic (Tumbling Window)
INSERT INTO store_revenue_stream
SELECT 
    store_id,
    TUMBLE_END(`row_time`, INTERVAL '1' MINUTE) as window_end,
    SUM(total_amount) as total_revenue
FROM raw_sales
GROUP BY 
    store_id, 
    TUMBLE(`row_time`, INTERVAL '1' MINUTE);

--设置hive-syntax为hive
SET table.sql-dialect=hive;


--创建hive结果表
CREATE TABLE hive_table (
      SESSION_ID STRING,
      APP_NO STRING,
      CUST_ID STRING,
      BUSINESS_TYPE_CD STRING,
      BUSINESS_TYPE_NAME STRING,
      CUST_SOURCE STRING,
      CHANNEL_SOURCE STRING,
      APPLY_TIME BIGINT
      ) STORED AS parquet 
	  TBLPROPERTIES (
	  'is_generic'='false',
      'sink.partition-commit.policy.kind'='metastore'
       );


--设置hive-syntax为default
SET table.sql-dialect=default;
--创建kafka数据源表
CREATE TABLE kafka_apply_info_106 (
      SESSION_ID STRING,
      APP_NO STRING,
      CUST_ID STRING,
      BUSINESS_TYPE_CD STRING,
      BUSINESS_TYPE_NAME STRING,
      CUST_SOURCE STRING,
      CHANNEL_SOURCE STRING,
      APPLY_TIME BIGINT
      --et AS TO_TIMESTAMP(FROM_UNIXTIME(APPLY_TIME/1000,'yyyy-MM-dd HH:mm:ss')),
      --WATERMARK FOR et AS et - INTERVAL '5' SECOND
    )
    WITH (
      'connector' = 'kafka',
      'topic' = 'kafkaCreditApplyInfo',
      'properties.group.id'='dev_flink',
	  'properties.zookeeper.connect'='10.1.30.6:2181',
      'properties.bootstrap.servers' = '10.1.30.8:9092',
      'format' = 'json',
      'scan.startup.mode' = 'latest-offset'
      );
	  
	  
--创建kafka测试表
CREATE TABLE kafka_apply_info_106_test (
      SESSION_ID STRING,
      APP_NO STRING,
      CUST_ID STRING,
      BUSINESS_TYPE_CD STRING,
      BUSINESS_TYPE_NAME STRING,
      CUST_SOURCE STRING,
      CHANNEL_SOURCE STRING,
      APPLY_TIME BIGINT
      --et AS TO_TIMESTAMP(FROM_UNIXTIME(APPLY_TIME/1000,'yyyy-MM-dd HH:mm:ss')),
      --WATERMARK FOR et AS et - INTERVAL '5' SECOND
    )
    WITH (
      'connector' = 'kafka',
      'topic' = 'kafkatestInfoApply',
      'properties.group.id'='dev_flink',
	  'properties.zookeeper.connect'='10.1.30.6:2181',
      'properties.bootstrap.servers' = '10.1.30.8:9092',
      'format' = 'json',
      'scan.startup.mode' = 'latest-offset'
      );

--插入Kafka测试表
INSERT INTO kafka_apply_info_106_test
SELECT
SESSION_ID,
APP_NO,
CUST_ID,
BUSINESS_TYPE_CD,
BUSINESS_TYPE_NAME,
CUST_SOURCE,
CHANNEL_SOURCE,
APPLY_TIME
--TO_TIMESTAMP(FROM_UNIXTIME(APPLY_TIME/1000,'yyyy-MM-dd HH:mm:ss')) AS APPLY_TIME
FROM kafka_apply_info_106
;



--插入hive表
INSERT INTO hive_table
SELECT
SESSION_ID,
APP_NO,
CUST_ID,
BUSINESS_TYPE_CD,
BUSINESS_TYPE_NAME,
CUST_SOURCE,
CHANNEL_SOURCE,
APPLY_TIME
--TO_TIMESTAMP(FROM_UNIXTIME(APPLY_TIME/1000,'yyyy-MM-dd HH:mm:ss')) AS APPLY_TIME
FROM kafka_apply_info_106
;
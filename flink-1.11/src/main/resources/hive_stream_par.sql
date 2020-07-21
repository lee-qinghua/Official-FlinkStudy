--设置hive-syntax为hive
SET table.sql-dialect=hive;


--创建hive结果表
CREATE TABLE apply_info (
      SESSION_ID STRING,
      APP_NO STRING,
      CUST_ID STRING,
      BUSINESS_TYPE_CD STRING,
      BUSINESS_TYPE_NAME STRING,
      CUST_SOURCE STRING,
      CHANNEL_SOURCE STRING,
      APPLY_TIME STRING
      ) 
	  PARTITIONED BY (dt STRING)
	  STORED AS parquet 
	  TBLPROPERTIES (
	  'partition.time-extractor.timestamp-pattern'='$dt',
	  'sink.partition-commit.trigger'='partition-time',
	  'sink.partition-commit.delay'='0s',
	  'is_generic'='false',
      'sink.partition-commit.policy.kind'='metastore'
       );


--设置hive-syntax为default
SET table.sql-dialect=default;
--创建kafka数据源表
CREATE TABLE kafka_apply_info_108 (
      SESSION_ID STRING,
      APP_NO STRING,
      CUST_ID STRING,
      BUSINESS_TYPE_CD STRING,
      BUSINESS_TYPE_NAME STRING,
      CUST_SOURCE STRING,
      CHANNEL_SOURCE STRING,
      APPLY_TIME BIGINT,
      et AS TO_TIMESTAMP(FROM_UNIXTIME(APPLY_TIME/1000,'yyyy-MM-dd HH:mm:ss')),
      WATERMARK FOR et AS et - INTERVAL '5' SECOND
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
	  
	  


--插入hive表
INSERT INTO apply_info
SELECT
SESSION_ID,
APP_NO,
CUST_ID,
BUSINESS_TYPE_CD,
BUSINESS_TYPE_NAME,
CUST_SOURCE,
CHANNEL_SOURCE,
FROM_UNIXTIME(APPLY_TIME/1000,'yyyy-MM-dd HH:mm:ss') AS APPLY_TIME,
FROM_UNIXTIME(APPLY_TIME/1000,'yyyy-MM-dd') AS dt
FROM kafka_apply_info_108
;
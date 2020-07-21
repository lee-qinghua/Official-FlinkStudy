--kafka授信申请入参表
    CREATE TABLE kafka_apply_info_107 (
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


--kafka授信结果入参表
   CREATE TABLE kafka_result_info_107 (
      SESSION_ID STRING,
      APP_NO  STRING,
      CUST_ID STRING,
      CREDIT_NO  STRING,
      BUSINESS_TYPE_CD STRING,
      STATE_CODE STRING,
      CREDIT_CODE  STRING,
      REFUSE_REASON STRING,
      INTEREST_RATE  DOUBLE,
      CREDIT_LIMIT DOUBLE,
     REPAY_MODE_CD STRING,
     LOAN_TERM INTEGER,
     CREDIT_SCORE_1  DOUBLE,
     CREDIT_SCORE_2 DOUBLE,
     CREDIT_SCORE_3  DOUBLE,
     ANTI_FRAUD_SCORE_1  DOUBLE,
     ANTI_FRAUD_SCORE_2  DOUBLE,
     CREDIT_TIME  BIGINT,
    et AS TO_TIMESTAMP(FROM_UNIXTIME(CREDIT_TIME/1000,'yyyy-MM-dd HH:mm:ss')),
    WATERMARK FOR et AS et - INTERVAL '5' SECOND
    )WITH (
        'connector' = 'kafka',
        'topic' = 'kafkaCreditResultInfo',
        'properties.group.id'='dev_flink',
		'properties.zookeeper.connect'='10.1.30.6:2181',
        'properties.bootstrap.servers' = '10.1.30.8:9092',
        'format' = 'json',
        'scan.startup.mode' = 'latest-offset'
    );
	
	
--kafka sink表
    CREATE TABLE InterCustMonitor_107 (
      SESSION_ID STRING,
      APP_NO STRING,
      CUST_ID STRING,
      BUSINESS_TYPE_CD STRING,
      BUSINESS_TYPE_NAME STRING,
      CUST_SOURCE STRING,
      CHANNEL_SOURCE STRING,
      APPLY_TIME BIGINT,
      CREDIT_NO STRING,
      STATE_CODE STRING,
      CREDIT_CODE STRING,
      REFUSE_REASON STRING,
      INTEREST_RATE DOUBLE,
      CREDIT_LIMIT DOUBLE,
      REPAY_MODE_CD STRING,
      LOAN_TERM INTEGER,
      CREDIT_SCORE_1 DOUBLE,
      CREDIT_SCORE_2 DOUBLE,
	  CREDIT_SCORE_3 DOUBLE,
	  ANTI_FRAUD_SCORE_1 DOUBLE,
	  ANTI_FRAUD_SCORE_2 DOUBLE,
	  CREDIT_TIME BIGINT
      )
       WITH (
      'connector' = 'kafka',
      'topic' = 'kafkaInterCustMonitor',
      'properties.group.id'='dev_flink',
	  'properties.zookeeper.connect'='10.1.30.6:2181',
      'properties.bootstrap.servers' = '10.1.30.8:9092',
      'format' = 'json',
      'scan.startup.mode' = 'latest-offset'
    );	


--插入表
	INSERT INTO InterCustMonitor_107
	SELECT
	apply.SESSION_ID AS SESSION_ID,
	apply.APP_NO AS APP_NO, 
	apply.CUST_ID AS CUST_ID, 
	apply.BUSINESS_TYPE_CD AS BUSINESS_TYPE_CD, 
	apply.BUSINESS_TYPE_NAME AS BUSINESS_TYPE_NAME, 
	apply.CUST_SOURCE AS CUST_SOURCE,
	apply.CHANNEL_SOURCE AS CHANNEL_SOURCE,
	apply.APPLY_TIME AS APPLY_TIME,
	results.CREDIT_NO AS CREDIT_NO,
	results.STATE_CODE AS STATE_CODE,
	results.CREDIT_CODE AS CREDIT_CODE,
	results.REFUSE_REASON AS REFUSE_REASON,
	results.INTEREST_RATE AS INTEREST_RATE,
	results.CREDIT_LIMIT AS CREDIT_LIMIT,
	results.REPAY_MODE_CD AS REPAY_MODE_CD,
	results.LOAN_TERM AS LOAN_TERM,
	results.CREDIT_SCORE_1 AS CREDIT_SCORE_1,
	results.CREDIT_SCORE_2 AS CREDIT_SCORE_2,
	results.CREDIT_SCORE_3 AS CREDIT_SCORE_3,
	results.ANTI_FRAUD_SCORE_1 AS ANTI_FRAUD_SCORE_1,
	results.ANTI_FRAUD_SCORE_2 AS ANTI_FRAUD_SCORE_2,
	results.CREDIT_TIME AS CREDIT_TIME
	FROM kafka_apply_info_107 apply
	JOIN kafka_result_info_107 results
	ON apply.SESSION_ID=results.SESSION_ID
	;





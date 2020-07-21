
 
  CREATE TABLE source_test1 (
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
  CREDIT_TIME  BIGINT
 )
 WITH (
 'connector.type' = 'kafka',
 'connector.version' = 'universal',
 'connector.topic' = 'kafkaCreditResultInfo',
 'connector.properties.zookeeper.connect' = '10.1.30.8:2181',
 'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
 'format.type' = 'json',
 'update-mode' = 'append'
 );
 
   CREATE TABLE sink_test1 (
 SESSION_ID STRING,
  APP_NO  STRING,
  CUST_ID STRING
 )
 WITH (
 'connector.type' = 'kafka',
 'connector.version' = 'universal',
 'connector.topic' = 'kafkaCreditResultInfo',
 'connector.properties.zookeeper.connect' = '10.1.30.8:2181',
 'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
 'format.type' = 'json',
 'update-mode' = 'append'
 );
 
 
 insert into sink_test1
 select
 APP_NO,
 CUST_ID from source_test1;
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
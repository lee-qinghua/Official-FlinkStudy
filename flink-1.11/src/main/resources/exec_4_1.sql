--kafka授信申请入参表
    CREATE TABLE kafka_apply_info_8 (
      SESSION_ID STRING,
      APP_NO STRING,
      CUST_ID STRING,
      BUSINESS_TYPE_CD STRING,
      BUSINESS_TYPE_NAME STRING,
      CUST_SOURCE STRING,
      CHANNEL_SOURCE STRING,
      APPLY_TIME BIGINT,
      et AS TO_TIMESTAMP(FROM_UNIXTIME(APPLY_TIME/1000)),
      WATERMARK FOR et AS et - INTERVAL '5' SECOND
    )
    WITH (
      'connector.type' = 'kafka',
      'connector.version' = 'universal',
      'connector.topic' = 'kafkaCreditApplyInfo',
      'connector.properties.group.id'='irce_flink',
      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
       'connector.properties.security.protocol' = 'SASL_PLAINTEXT',
      'connector.properties.sasl.mechanism' = 'PLAIN',
      'connector.properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required  username="irce"  password="R5c2_e2c";',
      'format.type' = 'json',
      'update-mode' = 'append'
      );

--kafka授信结果入参表
   CREATE TABLE kafka_result_info_8 (
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
    et AS TO_TIMESTAMP(FROM_UNIXTIME(CREDIT_TIME/1000)),
    WATERMARK FOR et AS et - INTERVAL '5' SECOND
    )WITH (
        'connector.type' = 'kafka',
        'connector.version' = 'universal',
        'connector.topic' = 'kafkaCreditResultInfo',
        'connector.properties.group.id'='irce_flink',
        'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
        'connector.properties.security.protocol' = 'SASL_PLAINTEXT',
      'connector.properties.sasl.mechanism' = 'PLAIN',
      'connector.properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required  username="irce"  password="R5c2_e2c";',
        'format.type' = 'json',
        'update-mode' = 'append'
    );

--kafka sink表
    CREATE TABLE kafka_product_monitor_2 (
      P_ID STRING,
      DT TIMESTAMP(3),
      CH INT,
      APP_C_PH BIGINT NOT NULL,
      R_C_PH BIGINT NOT NULL,
      ACC_C_PH BIGINT NOT NULL,
      APP_P_H FLOAT,
      APP_P_D FLOAT,
      PR FLOAT NOT NULL,
      PR_P_H FLOAT,
      PR_P_D FLOAT,
      AVG_CS_1 DOUBLE,
      AVG_CS_2 DOUBLE,
      AVG_CS_3 DOUBLE,
      AVG_AFS_1 DOUBLE,
      AVG_AFS_2 DOUBLE,
      AVG_LIMIT DOUBLE,
      AVG_RATE FLOAT
      )
       WITH (
      'connector.type' = 'kafka',
      'connector.version' = 'universal',
      'connector.topic' = 'kafkaProductMonitor',
      'connector.properties.group.id'='irce_flink',
     'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
       'connector.properties.security.protocol' = 'SASL_PLAINTEXT',
      'connector.properties.sasl.mechanism' = 'PLAIN',
     'connector.properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required  username="irce"  password="R5c2_e2c";',
      'format.type' = 'json',
      'update-mode' = 'append'
    );

--注册所需的udf
create function test_previous_long as 'com.cebbank.airisk.flink.udfs.PreviousValueAggFunction$LongPreviousValueAggFunction';

create function test_previous_float as 'com.cebbank.airisk.flink.udfs.PreviousValueAggFunction$FloatPreviousValueAggFunction';


CREATE VIEW kafka_result_info_view_minute AS SELECT
BUSINESS_TYPE_CD as product_ID,
COUNT(*) as R_C_PH,
COUNT(case when CREDIT_CODE='0' then 1 else NULL END) as ACC_C_PH,
TUMBLE_ROWTIME(et, INTERVAL '5' MINUTE) as cur_time,
avg(CREDIT_SCORE_1) as AVG_CS_1,
avg(CREDIT_SCORE_2) as AVG_CS_2,
avg(CREDIT_SCORE_3) as AVG_CS_3,
avg(ANTI_FRAUD_SCORE_1) as AVG_AFS_1,
avg(ANTI_FRAUD_SCORE_2) as AVG_AFS_2,
avg(INTEREST_RATE) as AVG_RATE,
avg(CREDIT_LIMIT) as AVG_LIMIT
FROM kafka_result_info_8
GROUP BY TUMBLE(et, INTERVAL '5' MINUTE) , BUSINESS_TYPE_CD;

CREATE VIEW kafka_result_info_view_hour AS
SELECT sum(ACC_C_PH) as ACC_C_PH,
sum(R_C_PH) as R_C_PH,
product_ID,
TUMBLE_ROWTIME(cur_time, INTERVAL '1' HOUR) as cur_time,
avg(AVG_CS_1) as AVG_CS_1,
avg(AVG_CS_2) as AVG_CS_2,
avg(AVG_CS_3) as AVG_CS_3,
avg(AVG_AFS_1) as AVG_AFS_1,
avg(AVG_AFS_2) as AVG_AFS_2,
avg(AVG_RATE) as AVG_RATE,
avg(AVG_LIMIT) as AVG_LIMIT
from kafka_result_info_view_minute
GROUP BY TUMBLE(cur_time, INTERVAL '1' HOUR) , product_ID;

CREATE VIEW kafka_apply_info_view_minute AS
SELECT
 TUMBLE_ROWTIME(et, INTERVAL '5' MINUTE) as cur_time,
 BUSINESS_TYPE_CD as product_ID,
 COUNT(*) as apply_num
FROM kafka_apply_info_8
GROUP BY TUMBLE(et, INTERVAL '5' MINUTE), BUSINESS_TYPE_CD;


CREATE VIEW kafka_apply_info_view_hour AS 
SELECT
HOUR(TUMBLE_START(cur_time, INTERVAL '1' HOUR)) as cur_hour,
TUMBLE_ROWTIME(cur_time, INTERVAL '1' HOUR) as cur_time,
product_ID,
sum(apply_num) as apply_num
FROM kafka_apply_info_view_minute
GROUP BY TUMBLE(cur_time, INTERVAL '1' HOUR), product_ID;


CREATE VIEW kafka_apply_info_view_hour_2 AS
SELECT
cur_time,
cur_hour,
apply_num,
product_ID,
test_previous_long(apply_num,1) OVER w AS last_num,
test_previous_long(apply_num,24) OVER w AS last_24_num,
DATE_FORMAT(cur_time, 'yyyy-MM-dd HH:mm:ss') AS data_time
from kafka_apply_info_view_hour
WINDOW w AS (PARTITION BY product_ID ORDER BY cur_time ROWS BETWEEN UNBOUNDED
PRECEDING AND CURRENT ROW);




CREATE VIEW kafka_apply_info_view_hour_3 AS
SELECT data_time,apply_num,product_ID,cur_hour,cur_time,
CAST(apply_num AS float)/CAST(last_num AS float) AS
 apply_compared_with_previous_hour,
CAST(apply_num AS float)/CAST(last_24_num AS float) AS
 apply_compared_with_previous_day
from kafka_apply_info_view_hour_2;


INSERT INTO kafka_product_monitor_2
SELECT 
o.product_ID as P_ID,
 CAST( o.data_time AS TIMESTAMP(3)) AS DT,
 CAST(o.cur_hour AS INT) AS CH,
 o.apply_num as APP_C_PH,
 s.R_C_PH as R_C_PH,
 s.ACC_C_PH as ACC_C_PH,
 o.apply_compared_with_previous_hour as APP_P_H,
 o.apply_compared_with_previous_day as APP_P_D,
 CAST(s.ACC_C_PH AS float)/CAST(o.apply_num AS float) AS PR,
 CAST(CAST(s.ACC_C_PH AS float)/CAST( o.apply_num AS float) as float)/CAST(test_previous_float(CAST(s.ACC_C_PH AS float)/CAST( o.apply_num AS float),1) OVER w as float) as PR_P_H,
 CAST(CAST(s.ACC_C_PH AS float)/CAST( o.apply_num AS float) as float)/CAST(test_previous_float(CAST(s.ACC_C_PH AS float)/CAST( o.apply_num AS float),4) OVER w  as float) as PR_P_D,
 s.AVG_CS_1 as AVG_CS_1,
 s.AVG_CS_2 as AVG_CS_2,
 s.AVG_CS_3 as AVG_CS_3,
 s.AVG_AFS_1 as AVG_AFS_1,
 s.AVG_AFS_2 as AVG_AFS_2,
 s.AVG_LIMIT as AVG_LIMIT,
 CAST(s.AVG_RATE AS FLOAT) AS AVG_RATE
FROM kafka_apply_info_view_hour_3 o,kafka_result_info_view_hour s
WHERE o.product_ID = s.product_ID AND
 o.cur_time BETWEEN s.cur_time - INTERVAL '2' SECOND AND s.cur_time +
INTERVAL '2' SECOND WINDOW w AS (PARTITION BY o.product_ID ORDER BY o.cur_time ROWS BETWEEN UNBOUNDED
PRECEDING AND CURRENT ROW);
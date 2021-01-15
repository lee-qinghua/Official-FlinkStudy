package com.otis.demo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Monitor {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String apply = "CREATE TABLE kafka_apply_info_200 (\n" +
                "      SESSION_ID STRING,\n" +
                "      APP_NO STRING,\n" +
                "      CUST_ID STRING,\n" +
                "      BUSINESS_TYPE_CD STRING,\n" +
                "      BUSINESS_TYPE_NAME STRING,\n" +
                "      CUST_SOURCE STRING,\n" +
                "      CHANNEL_SOURCE STRING,\n" +
                "      APPLY_TIME BIGINT,\n" +
                "      et AS TO_TIMESTAMP(FROM_UNIXTIME(APPLY_TIME/1000)),\n" +
                "      WATERMARK FOR et AS et - INTERVAL '5' SECOND\n" +
                "    )\n" +
                "    WITH (\n" +
                "      'connector.type' = 'kafka',\n" +
                "\t  'connector.startup-mode'='latest-offset',\n" +
                "    'connector.version' = 'universal',\n" +
                "      'connector.topic' = 'kafkaCreditApplyInfo_1234',\n" +
                "      'connector.properties.group.id'='dev_flink',\n" +
                "      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',\n" +
                "      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',\n" +
                "      'format.type' = 'json',\n" +
                "      'update-mode' = 'append'\n" +
                "      )";
        tableEnv.executeSql(apply);
        String result = " CREATE TABLE kafka_result_info_200 (\n" +
                "      SESSION_ID STRING,\n" +
                "      APP_NO  STRING,\n" +
                "      CUST_ID STRING,\n" +
                "      CREDIT_NO  STRING,\n" +
                "      BUSINESS_TYPE_CD STRING,\n" +
                "      STATE_CODE STRING,\n" +
                "      CREDIT_CODE  STRING,\n" +
                "      REFUSE_REASON STRING,\n" +
                "      INTEREST_RATE  DOUBLE,\n" +
                "      CREDIT_LIMIT DOUBLE,\n" +
                "     REPAY_MODE_CD STRING,\n" +
                "     LOAN_TERM INTEGER,\n" +
                "     CREDIT_SCORE_1  DOUBLE,\n" +
                "     CREDIT_SCORE_2 DOUBLE,\n" +
                "     CREDIT_SCORE_3  DOUBLE,\n" +
                "     ANTI_FRAUD_SCORE_1  DOUBLE,\n" +
                "     ANTI_FRAUD_SCORE_2  DOUBLE,\n" +
                "     CREDIT_TIME  BIGINT,\n" +
                "    et AS TO_TIMESTAMP(FROM_UNIXTIME(CREDIT_TIME/1000)),\n" +
                "    WATERMARK FOR et AS et - INTERVAL '5' SECOND\n" +
                "    )WITH (\n" +
                "      'connector.type' = 'kafka',\n" +
                "    'connector.version' = 'universal',\n" +
                "      'connector.topic' = 'kafkaCreditResultInfo_1234',\n" +
                "      'connector.properties.group.id'='dev_flink',\n" +
                "      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',\n" +
                "      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',\n" +
                "      'format.type' = 'json',\n" +
                "      'update-mode' = 'append'\n" +
                "    )";
        tableEnv.executeSql(result);
        String sink = "CREATE TABLE kafka_product_monitor_200 (\n" +
                "      P_ID STRING,\n" +
                "      DT TIMESTAMP(3),\n" +
                "      CH INT,\n" +
                "      APP_C_PH BIGINT NOT NULL,\n" +
                "      R_C_PH BIGINT NOT NULL,\n" +
                "      ACC_C_PH BIGINT NOT NULL,\n" +
                "      APP_P_H FLOAT,\n" +
                "      APP_P_D FLOAT,\n" +
                "      PR FLOAT NOT NULL,\n" +
                "      PR_P_H FLOAT,\n" +
                "      PR_P_D FLOAT,\n" +
                "      AVG_CS_1 DOUBLE,\n" +
                "      AVG_CS_2 DOUBLE,\n" +
                "      AVG_CS_3 DOUBLE,\n" +
                "      AVG_AFS_1 DOUBLE,\n" +
                "      AVG_AFS_2 DOUBLE,\n" +
                "      AVG_LIMIT DOUBLE,\n" +
                "      AVG_RATE FLOAT\n" +
                "      )\n" +
                "       WITH (\n" +
                "      'connector.type' = 'kafka',\n" +
                "    'connector.version' = 'universal',\n" +
                "      'connector.topic' = 'kafkaProductMonitor_123',\n" +
                "      'connector.properties.group.id'='dev_flink',\n" +
                "      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',\n" +
                "      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',\n" +
                "      'format.type' = 'json',\n" +
                "      'update-mode' = 'append'\n" +
                "    )";
        tableEnv.executeSql(sink);
        String view1 = "CREATE VIEW kafka_apply_info_hop_views AS\n" +
                "SELECT\n" +
                "BUSINESS_TYPE_CD,\n" +
                "HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR) AS dt,\n" +
                "HOUR(HOP_END(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR)) AS cur_hour,\n" +
                "COUNT(*) as apply_num\n" +
                "FROM kafka_apply_info_200\n" +
                "GROUP BY HOP(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR),BUSINESS_TYPE_CD";
        tableEnv.executeSql(view1);
        String print="create table print(a string)with('connector'='print')";
        tableEnv.executeSql(print);
        tableEnv.executeSql("insert into print select cast(apply_num as string) from kafka_apply_info_hop_views");
    }
}

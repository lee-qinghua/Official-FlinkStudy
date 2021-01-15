package com.otis.demo;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class QuickStart {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        
        String a = "    CREATE TABLE kafka_apply_info_200 (\n" +
                "      SESSION_ID STRING,\n" +
                "      APP_NO STRING,\n" +
                "      CUST_ID STRING,\n" +
                "      BUSINESS_TYPE_CD STRING,\n" +
                "      BUSINESS_TYPE_NAME STRING,\n" +
                "      CUST_SOURCE STRING,\n" +
                "      CHANNEL_SOURCE STRING,\n" +
                "      APPLY_TIME BIGINT\n" +
                "    )\n" +
                "    WITH (\n" +
                "      'connector.type' = 'kafka',\n" +
                "    'connector.version' = 'universal',\n" +
                "'connector.startup-mode' = 'latest-offset',\n" +
                "      'connector.topic' = 'kafkaCreditApplyInfo_1q',\n" +
                "      'connector.properties.group.id'='dev_flink',\n" +
                "      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',\n" +
                "      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',\n" +
                "      'format.type' = 'json',\n" +
                "      'update-mode' = 'append'\n" +
                "      )";
        tableEnv.executeSql(a);

        String b = "create table print_table(\n" +
                "session string,\n" +
                "app_no string\n" +
                ")with(\n" +
                "'connector'='print'\n" +
                ")";
        tableEnv.executeSql(b);

        tableEnv.executeSql("insert into print_table select SESSION_ID,APP_NO from kafka_apply_info_200");

//        //todo 创建表的sql
//        String source_table = " CREATE TABLE mytest (\n" +
//                "action STRING,\n" +
//                "age BIGINT,\n" +
//                "address STRING,\n" +
//                "ts BIGINT,\n" +
//                "et AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)),\n" +
//                "WATERMARK FOR et AS et - INTERVAL '5' SECOND" +
//                "    )\n" +
//                "    WITH (\n" +
//                "      'connector.type' = 'aaa',\n" +
//                "      'connector.version' = 'universal',\n" +
//                "      'connector.topic' = 'mytopic',\n" +
//                "      'connector.properties.zookeeper.connect' = '10.1.30.8:2181',\n" +
//                "      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',\n" +
//                "      'format.type' = 'json',\n" +
//                "      'update-mode' = 'append'\n" +
//                "      )";
//
//        String sink_table = " CREATE TABLE kafka_apply_info_9 (\n" +
//                "      action STRING,\n" +
//                "      counta BIGINT\n" +
//                "    )\n" +
//                "    WITH (\n" +
//                "      'connector.type' = 'kafka',\n" +
//                "      'connector.version' = 'universal',\n" +
//                "      'connector.topic' = 'sink_test',\n" +
//                "      'connector.properties.zookeeper.connect' = '10.1.30.8:2181',\n" +
//                "      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',\n" +
//                "      'format.type' = 'json',\n" +
//                "      'update-mode' = 'append'\n" +
//                "      )";
//
//
//        //todo 创建视图
//        String viewsql = "create view haha as\n" +
//                "select\n" +
//                "action,\n" +
//                "count(action) as counta \n" +
//                "from mytest\n" +
//                "group by action,tumble(et, interval '10' second)";
//
//
//        String insert_sql = "insert into kafka_apply_info_9 " +
//                "select\n" +
//                "action,\n" +
//                "counta \n" +
//                "from haha\n";
//        tableEnv.executeSql(source_table);
//        tableEnv.executeSql("select * from mytest");
//        tableEnv.executeSql(viewsql);
//        tableEnv.executeSql(insert_sql);
    }
}

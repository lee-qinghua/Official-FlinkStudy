package com.otis.demo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Monitor2 {
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
//        String view1 = "CREATE VIEW kafka_apply_info_hop_views AS\n" +
//                "SELECT\n" +
//                "BUSINESS_TYPE_CD,\n" +
//                "HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR) AS dt,\n" +
//                "HOUR(HOP_END(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR)) AS cur_hour,\n" +
//                "COUNT(*) as apply_num\n" +
//                "FROM kafka_apply_info_200\n" +
//                "GROUP BY HOP(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR),BUSINESS_TYPE_CD";
//        tableEnv.executeSql(view1);
//        String view1 = "create view aaa as select \n" +
//                "BUSINESS_TYPE_CD,\n" +
//                "HOP_ROWTIME(et,interval '10' second,interval '1' hour) as dt,\n" +
//                "count(*) as apply_num\n" +
//                "from kafka_apply_info_200 group by \n" +
//                "HOP(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR),BUSINESS_TYPE_CD";
//        tableEnv.executeSql(view1);
        String print = "create table print(a string,b bigint)with('connector'='print')";
        tableEnv.executeSql(print);
        tableEnv.executeSql("insert into print select BUSINESS_TYPE_CD,count(*) from kafka_apply_info_200 group by HOP(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR),BUSINESS_TYPE_CD");
    }
}

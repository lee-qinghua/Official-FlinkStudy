package com.otis.test;

import com.otis.udfs.DemoUdf;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.*;

/**
 * 单纯的从一张表到另一张表是ok的
 */
public class FunctionTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        DataStreamSource<String> dataStream = env.socketTextStream("10.1.30.10", 7777);
        String source_table = " CREATE TABLE mytest (\n" +
                "      action STRING,\n" +
                "      age INTEGER,\n" +
                " address STRING,\n" +
                " ts BIGINT\n" +
                "    )\n" +
                "    WITH (\n" +
                "      'connector.type' = 'kafka',\n" +
                "      'connector.version' = 'universal',\n" +
                "      'connector.topic' = 'mytopic',\n" +
                "      'connector.properties.zookeeper.connect' = '10.1.30.8:2181',\n" +
                "      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',\n" +
                "\n" +
                "      'format.type' = 'json',\n" +
                "      'update-mode' = 'append'\n" +
                "      )";
        String sink_table = " CREATE TABLE sinktest (\n" +
                "      action STRING,\n" +
                "      age INTEGER,\n" +
                " address STRING,\n" +
                " ts BIGINT\n" +
                "    )\n" +
                "    WITH (\n" +
                "      'connector.type' = 'kafka',\n" +
                "      'connector.version' = 'universal',\n" +
                "      'connector.topic' = 'sinktest',\n" +
                "      'connector.properties.zookeeper.connect' = '10.1.30.8:2181',\n" +
                "      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',\n" +
                "\n" +
                "      'format.type' = 'json',\n" +
                "      'update-mode' = 'append'\n" +
                "      )";
        tableEnv.createFunction("myfunction", DemoUdf.class );
        tableEnv.executeSql(source_table);
        tableEnv.executeSql(sink_table);
        TableResult tableResult = tableEnv.executeSql("insert into sinktest select action" +
                ",myfunction(age,1,-1) over (partition by action order by ts) as hehe from mytest");
    }
}

package com.otis.demo;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class QuickStart2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String datagen="create table data(\n" +
                "name string,\n" +
                "address string,\n" +
                "age BIGINT\n" +
                ")with(\n" +
                "'connector'='datagen',\n" +
                " 'rows-per-second' = '500'\n" +
                ")";
        tableEnv.executeSql(datagen);

        String b = "create table print_table(\n" +
                "name string,\n" +
                "address string,\n" +
                "age BIGINT\n" +
                ")with(\n" +
                "'connector'='print'\n" +
                ")";
        tableEnv.executeSql(b);

        tableEnv.executeSql("insert into print_table select * from data");

    }
}

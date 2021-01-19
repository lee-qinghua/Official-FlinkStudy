package com.otis.clickhouse;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;

/**
 * 可以根据规定的时间和批量的大小插入数据
 * 目前有重复插入的问题
 */
public class FlinkJob2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        String datagen = "create table data(\n" +
                "name string,\n" +
                "address string,\n" +
                "age int\n" +
                ")with(\n" +
                "'connector'='datagen',\n" +
                " 'rows-per-second' = '1'\n" +
                ")";
        tableEnv.executeSql(datagen);

        Table table = tableEnv.sqlQuery("select name,address,abs(age) from data");
        DataStream<Row> inputStream = tableEnv.toAppendStream(table, Row.class);

        String sql = "INSERT INTO user2 (name, address, age) VALUES (?,?,?)";
        String[] tableColums = {"name", "address", "age"};
        String[] types = {"string", "string", "int"};
        List<String> sqlTypes = Arrays.asList(types);
        // 集群地址
        String[] ips = {"10.1.30.10"};

        inputStream.addSink(new MyClickhouseSink2(sql, tableColums, sqlTypes, ips, "default", "", "qinghua"));

        env.execute("clickhouse sink test");
    }
}
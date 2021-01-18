package com.otis.clickhouse;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * countwindow 计数，达到规定的大小批量写入
 */
public class FlinkJob {
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
                " 'rows-per-second' = '50000'\n" +
                ")";
        tableEnv.executeSql(datagen);

        Table table = tableEnv.sqlQuery("select name,address,abs(age) as age from data");
        DataStream<User> inputStream = tableEnv.toAppendStream(table, User.class);

        AllWindowedStream<User, GlobalWindow> userGlobalWindowAllWindowedStream = inputStream.countWindowAll(50000);
        DataStream<List<User>> apply = userGlobalWindowAllWindowedStream.apply(new MyWindowFunction());
        // sink
        String sql = "INSERT INTO user (name, address, age) VALUES (?,?,?)";
        MyClickHouseUtil jdbcSink = new MyClickHouseUtil(sql);
        apply.addSink(jdbcSink);

        env.execute("clickhouse sink test");
    }

    static class MyWindowFunction implements AllWindowFunction<User, List<User>, GlobalWindow> {


        @Override
        public void apply(GlobalWindow window, Iterable<User> values, Collector<List<User>> out) throws Exception {
            List<User> users = new ArrayList<>();
            values.forEach(x -> users.add(x));
            out.collect(users);
        }
    }
}
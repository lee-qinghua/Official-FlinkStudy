package com.otis.work.date20200902udf;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.function.Consumer;

public class TestFunction {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        DataStream<String> textStream = env.socketTextStream("10.1.30.10", 7777);
        DataStream<Integer> mapStream = textStream.map(new MapFunction<String, Integer>() {

            @Override
            public Integer map(String s) throws Exception {
                return Integer.parseInt(s);
            }
        });
        Table amount = tableEnv.fromDataStream(mapStream, "amount");
        Table select = amount.select("*");



        tableEnv.createTemporaryView("mytable", mapStream);
        TableResult tableResult = tableEnv.executeSql("select * from mytable");
        CloseableIterator<Row> collect = tableResult.collect();
        collect.forEachRemaining(new Consumer<Row>() {
            @Override
            public void accept(Row row) {
                System.out.println(row.toString());
            }
        });
        env.execute("hha");
    }
}

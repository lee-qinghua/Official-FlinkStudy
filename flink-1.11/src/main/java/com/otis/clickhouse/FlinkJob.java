package com.otis.clickhouse;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        // source
        DataStream<String> inputStream = env.socketTextStream("10.1.30.10", 7777);

        // Transform 操作
        SingleOutputStreamOperator<User> dataStream = inputStream.map(new MapFunction<String, User>() {
            @Override
            public User map(String data) throws Exception {
                String[] split = data.split(",");
                return new User(split[0],
                        split[1],
                        Integer.parseInt(split[2]));
            }
        });

        // sink
        String sql = "INSERT INTO user (name, address, age) VALUES (?,?,?)";
        MyClickHouseUtil jdbcSink = new MyClickHouseUtil(sql);
        dataStream.addSink(jdbcSink);
        dataStream.print();

        env.execute("clickhouse sink test");
    }
}

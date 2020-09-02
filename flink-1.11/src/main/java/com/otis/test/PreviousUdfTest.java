package com.otis.test;


import com.otis.udfs.PreviousUdf;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class PreviousUdfTest {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        DataStreamSource<String> dataStream = env.socketTextStream("10.1.30.10", 7777);

        DataStream<Stu> parsed = dataStream.map(new MapFunction<String, Stu>() {
            @Override
            public Stu map(String value) {
                String[] arr = value.split(",");
                Stu stu = new Stu(arr[0], Integer.parseInt(arr[1]), Long.parseLong(arr[2]) * 1000);
                return stu;
            }
        });

        //创建视图
        tableEnv.createTemporaryView("mytable", parsed);

        //注册函数
        tableEnv.createFunction("myFunction", PreviousUdf.class);

        //输出表
        String print_table = "" +
                "create TABLE print_table(" +
                " rulecode string," +
                " amount1 int," +
                " amount2 int" +
                "        ) WITH (" +
                "          'connector' = 'print'" +
                "        )";
        tableEnv.executeSql(print_table);


        //执行sql
        tableEnv.sqlQuery("insert into print_table select * from mytable");


        env.execute();
//        String source_table = " CREATE TABLE mytest (\n" +
//                "      action STRING,\n" +
//                "      age INTEGER,\n" +
//                "      address STRING,\n" +
//                "      ts BIGINT\n" +
//                "    )\n" +
//                "    WITH (\n" +
//                "      'connector.type' = 'kafka',\n" +
//                "      'connector.version' = 'universal',\n" +
//                "      'connector.topic' = 'mytopic',\n" +
//                "      'connector.properties.zookeeper.connect' = '10.1.30.8:2181',\n" +
//                "      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',\n" +
//                "\n" +
//                "      'format.type' = 'json',\n" +
//                "      'update-mode' = 'append'\n" +
//                "      )";
//
//        //输出表
//        String print_table = "" +
//                "create TABLE print_table(" +
//                " rulecode int," +
//                " amount1 int," +
//                " amount2 boolean" +
//                "        ) WITH (" +
//                "          'connector' = 'print'" +
//                "        )";
//        tableEnv.executeSql(print_table);
//
//
//        tableEnv.createFunction("myfunction", PreviousUdf.class);
//        tableEnv.executeSql(source_table);
//
//        TableResult tableResult = tableEnv.executeSql("insert into sinktest select action" +
//                ",myfunction(age,1,-1) as lastvalue from mytest");
    }
}

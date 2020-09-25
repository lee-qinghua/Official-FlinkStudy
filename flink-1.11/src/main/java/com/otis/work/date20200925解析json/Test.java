package com.otis.work.date20200925解析json;

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

public class Test {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

//        String source_table = "create table test(\n" +
//                "name string,\n" +
//                "age bigint\n" +
//                ")with(\n" +
//                "  'connector' = 'filesystem',\n" +
//                "  'path' = 'file:///D:\\peoject\\Official-FlinkStudy\\flink-1.11\\src\\main\\java\\com\\otis\\work\\date20200925解析json\\d.json',\n" +
//                "  'format' = 'json'                                  \n" +
//                ")";
//
//        String source_table2 = "create table test2(\n" +
//                "name string,\n" +
//                "age bigint\n" +
//                ")with(\n" +
//                "  'connector' = 'filesystem',\n" +
//                "  'path' = 'file:///D:\\peoject\\Official-FlinkStudy\\flink-1.11\\src\\main\\java\\com\\otis\\work\\date20200925解析json\\d.json',\n" +
//                "  'format' = 'json'                                  \n" +
//                ")";
//        tableEnv.executeSql(source_table);

        // todo 解析这种格式的json "爱好":{"movie":"haha","food":"huolongguo"}  这样写成 hobies Row(movie string,food string)
        // todo sql中取movie的时候 直接  hobies.movie
//        String s3="create table test3(\n" +
//                "  name string,\n" +
//                "  age bigint,\n" +
//                "  hobies ROW(movie string,food string)\n" +
//                ")with(\n" +
//                "'connector' = 'filesystem',\n" +
//                "'path' = 'file:///D:\\peoject\\Official-FlinkStudy\\flink-1.11\\src\\main\\java\\com\\otis\\work\\date20200925解析json\\d.json',\n" +
//                "'format' = 'json'\n" +
//                ")";
//        tableEnv.executeSql(s3);


        //注册一个打印表
//        String print_table3 = " CREATE TABLE print_table3 (" +
//                "         name STRING," +
//                "         age bigint," +
//                "         movie string," +
//                "         food string" +
//                "        ) WITH (" +
//                "          'connector' = 'print'" +
//                "        )";
//        tableEnv.executeSql(print_table3);
//        tableEnv.executeSql("insert into print_table3 select name,age,hobies.movie,hobies.food from test3");

        //todo company:[{"name":"公司1","time":"1"},{"name":"公司2","time":"2"}]
        //todo 这样直接映射成 ARRAY<ROW(mname string,mtime string)>
        String s4="create table test4(\n" +
                "  name string,\n" +
                "  age bigint,\n" +
                "  hobies ROW(movie string,food string),\n" +
                "  company ARRAY<ROW(mname string,mtime string)>\n" +
                ")with(\n" +
                "'connector' = 'filesystem',\n" +
                "'path' = 'file:///D:\\peoject\\Official-FlinkStudy\\flink-1.11\\src\\main\\java\\com\\otis\\work\\date20200925解析json\\d.json',\n" +
                "'format' = 'json'\n" +
                ")\n";

        tableEnv.executeSql(s4);
        //注册一个打印表
        String print_table4 = " CREATE TABLE print_table4 (" +
                "         name STRING," +
                "         age bigint," +
                "         movie string," +
                "         food string," +
                "         marray ARRAY<ROW(mname string,mtime string)>" +
                "        ) WITH (" +
                "          'connector' = 'print'" +
                "        )";
        tableEnv.executeSql(print_table4);

        tableEnv.executeSql("insert into print_table4 select name,age,hobies.movie,hobies.food,company from test4");

    }
}

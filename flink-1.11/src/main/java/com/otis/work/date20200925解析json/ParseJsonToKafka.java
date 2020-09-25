package com.otis.work.date20200925解析json;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 作者：李清华
 * 功能：解析json报文 生成表 发送到kafka
 * 日期：2020/9/25-13:48
 */

public class ParseJsonToKafka {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
        env.readTextFile("D:\\peoject\\Official-FlinkStudy\\flink-1.11\\src\\main\\java\\com\\otis\\work\\date20200925解析json\\d.json");
    }
}

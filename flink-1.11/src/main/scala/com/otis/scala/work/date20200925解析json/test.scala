package com.otis.scala.work.date20200925解析json

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row


object test {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val bsSettings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tableEnv = StreamTableEnvironment.create(env, bsSettings)
    val source_table = "create table test(\n" +
      "name string,\n" +
      "age bigint\n" +
      ")with(\n" +
      "  'connector' = 'filesystem',\n" +
      "  'path' = 'file:///D:\\peoject\\Official-FlinkStudy\\flink-1.11\\src\\main\\scala\\com\\otis\\scala\\work\\date20200925解析json\\a.json',\n" +
      "  'format' = 'json'                                  \n" +
      ")"
    tableEnv.executeSql(source_table)
    tableEnv.sqlQuery("select * from test").toAppendStream[Row].print()
    env.execute()
  }
}

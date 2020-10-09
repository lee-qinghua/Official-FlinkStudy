package com.otis.scala.work.date20200925解析json

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row


object test3 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val bsSettings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bsSettings)

    val ods_table =
      """
        |create table ods_table(
        |a string,
        |b string
        |)
        |with(
        |'connector' = 'kafka',
        |'topic' = 'qinghua002_source_table_a02',
        |'properties.bootstrap.servers' = '10.1.30.8:9092',
        |'format' = 'json',
        |'scan.startup.mode' = 'earliest-offset'
        |)
        |""".stripMargin
    tableEnv.executeSql(ods_table)
    tableEnv.sqlQuery("select * from ods_table").toAppendStream[Row].print()
    //输出到kafka
    val sink_table4 =
      """
        |CREATE TABLE sink_table4 (
        |    a string,
        |    b string
        |    )
        |    WITH (
        |      'connector' = 'kafka',
        |      'topic' = 'qinghuatest-001',
        |      'properties.group.id'='dev_flink',
        |      'properties.zookeeper.connect'='10.1.30.6:2181',
        |      'properties.bootstrap.servers' = '10.1.30.8:9092',
        |      'format' = 'json',
        |      'scan.startup.mode' = 'latest-offset'
        |      )
        |""".stripMargin
    tableEnv.executeSql(sink_table4)
    tableEnv.executeSql("insert into sink_table4 select * from ods_table")
    env.execute("aaa")
  }
}

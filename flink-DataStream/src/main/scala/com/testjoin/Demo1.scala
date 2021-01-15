package com.testjoin

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object Demo1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val bsSettings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bsSettings)

    // 创建kafka流表
    tableEnv.executeSql(
      """
        |create table clikc_table(
        |    ad_id string COMMENT '广告位唯一ID',
        |    user_id string COMMENT '用户ID',
        |    click_ts bigint,
        |    procTime AS PROCTIME(),
        |    ets AS TO_TIMESTAMP(FROM_UNIXTIME(click_ts / 1000)),
        |    WATERMARK FOR ets AS ets - INTERVAL '1' MINUTE
        |)
        |with(
        | 'connector' = 'kafka',
        | 'topic' = 'click',
        | 'properties.bootstrap.servers' = 'hadoop:9092',
        | 'properties.group.id' = 'test',
        | 'format' = 'json',
        | 'scan.startup.mode' = 'latest-offset'
        |)
        |""".stripMargin)

    tableEnv.executeSql(
      """
        |create table ad_name(
        |id string,
        |name string
        |) WITH (
        |   'connector' = 'jdbc',
        |   'url' = 'jdbc:mysql://hadoop:3306/flink',
        |   'table-name' = 'testdim',
        |   'driver'     = 'com.mysql.jdbc.Driver',
        |   'username' = 'root',
        |  'password' = 'root'
        |)""".stripMargin)

    tableEnv.executeSql(
      """
        |create table print_1(id string,name string) with('connector'='print')
        |""".stripMargin)

    //tableEnv.executeSql("insert into print_1 select * from ad_name")

    tableEnv.executeSql(
      """
        |create table print_2(id string,username string,adname string) with('connector'='print')
        |""".stripMargin)
    tableEnv.executeSql(
      """
        |insert into print_2
        |select
        |ad_id,
        |user_id,
        |name
        |from clikc_table a join ad_name for system_time as of a.procTime b on a.ad_id=b.id
        |""".stripMargin)

  }
}

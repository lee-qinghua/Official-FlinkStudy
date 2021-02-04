package com.otis.借款熔断

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._

/**
 *
 * 根据消息中的借款/还款标志，分为两个topic
 * 借款 给数据加上还款日期，有无还款标志，是否逾期标志,存库
 * 还款直接发送到下个topic
 */

object A_SplitAndEtl {

  // repayment_flag  0 未还款 1 已还款
  // overtime        0 未逾期 1 已逾期
  case class Jie(id: String, flag: String, event_time: String, end_time: String, repayment_flag: String, overtime_flag: String)

  case class OdsData(id: String, flag: String, event_time: String)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 传过来的只有id flag(0借款/1还款) event_time
    val stream = env.socketTextStream("10.1.30.10", 7777)

    val stream1 = stream.map(x => {
      val arr = x.split(" ")
      val id = arr(0)
      val flag = arr(1)
      val event_time = arr(2)
      OdsData(id, flag, event_time)
    })

    //------------------------------
    // 借款流处理
    //------------------------------
    val stream2_jie = stream1.filter(_.flag == "0").map(x => {
      val id = x.id
      val flag = x.flag
      val event_time = x.event_time
      val end_time = (90 * 24 * 60 * 60 + x.event_time.toLong).toString
      val repayment_flag = "0"
      val overtime_flag = "0"
      Jie(id, flag, event_time, end_time, repayment_flag, overtime_flag)
    })
    val table = tableEnv.fromDataStream(stream2_jie)
    tableEnv.createTemporaryView("jie", table)

    val table_jie =
      """
        |create table table_jie(
        |id string,
        |flag string,
        |event_time bigint,
        |end_time bigint,
        |repayment_flag string,
        |overtime_flag string
        |)with(
        |  'connector' = 'kafka',
        |  'topic' = 'qinghua_jie',
        |  'properties.bootstrap.servers' = '10.1.30.8:9092',
        |  'properties.group.id' = 'dev_flink',
        |  'scan.startup.mode' = 'latest-offset',
        |  'format' = 'json'
        |)
        |""".stripMargin

    tableEnv.executeSql(table_jie)
    tableEnv.executeSql("insert into table_jie select id,flag,cast(event_time as bigint),cast(end_time as bigint),repayment_flag,overtime_flag from jie")


    //------------------------------
    // 借款和还款的数据发送到下游
    //------------------------------
    val table2 = tableEnv.fromDataStream(stream1)
    tableEnv.createTemporaryView("all_data", table2)
    val table_all_data =
      """
        |create table table_all_data(
        |id string,
        |flag string,
        |event_time bigint
        |)with(
        |  'connector' = 'kafka',
        |  'topic' = 'table_all_data',
        |  'properties.bootstrap.servers' = '10.1.30.8:9092',
        |  'properties.group.id' = 'dev_flink',
        |  'scan.startup.mode' = 'latest-offset',
        |  'format' = 'json'
        |)
        |""".stripMargin
    tableEnv.executeSql(table_all_data)
    tableEnv.executeSql("insert into table_all_data select id,flag,cast(event_time as bigint) from all_data")

  }
}

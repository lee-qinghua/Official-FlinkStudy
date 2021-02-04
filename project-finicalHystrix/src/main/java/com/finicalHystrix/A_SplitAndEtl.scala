package com.finicalHystrix

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._


/**
 * 使用文档：https://bahir.apache.org/docs/flink/current/flink-streaming-kudu/
 * 原始数据的清晰和分流，把借款的数据抽出到一个新流中，原始流也保留借款数据
 *
 */
object A_SplitAndEtl {

  // repayment_flag  0 未还款 1 已还款
  // overtime        0 未逾期 1 已逾期
  case class Jie(relation_id: String, event_type: String, user_id: String, amt: Float, event_dt: Long, should_repay_time: Long, repay_state: String, actual_repay_time: Long)

  // kafka中的原始消息
  case class OdsData(event_id: String, relation_id: String, event_type: String, user_id: String, amt: Float, event_dt: Long, should_repay_time: Long, repay_state: String, actual_repay_time: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)


    // 传过来的只有id flag(0借款/1还款) event_time
    val stream = env.socketTextStream("10.1.30.10", 7777)

    val stream1 = stream.map(x => {
      val arr = x.split(",")
      val event_id = arr(0)
      val relation_id = arr(1)
      val event_type = arr(2)
      val user_id = arr(3)
      val amt = arr(4).toFloat
      val event_dt = arr(5).toLong
      val should_repay_time = arr(6).toLong
      val repay_state = arr(7)
      val actual_repay_time = arr(8).toLong
      OdsData(event_id, relation_id, event_type, user_id, amt, event_dt, should_repay_time, repay_state, actual_repay_time)
    })


    //------------------------------
    // 借款流处理
    //------------------------------
    val stream2_jie = stream1.filter(_.event_type == "0").map(x => {
      val relation_id = x.relation_id
      val event_type = x.event_type
      val user_id = x.user_id
      val amt = x.amt
      val event_dt = x.event_dt
      val should_repay_time = x.should_repay_time
      val repay_state = x.repay_state
      val actual_repay_time = x.actual_repay_time
      Jie(relation_id, event_type, user_id, amt, event_dt, should_repay_time, repay_state, actual_repay_time)
    })
    val table = tableEnv.fromDataStream(stream2_jie)
    tableEnv.createTemporaryView("jie", table)

    val table_jie =
      """
        |create table table_jie(
        |relation_id	string,
        |event_type	string,
        |user_id	string,
        |amt float,
        |event_dt	bigint,
        |should_repay_time	bigint,
        |repay_state	string,
        |actual_repay_time	bigint
        |)with(
        |  'connector' = 'kafka',
        |  'topic' = 'qinghua_jie',
        |  'properties.bootstrap.servers' = '10.1.30.8:9092',
        |  'properties.group.id' = 'dev_flink',
        |  'format' = 'json'
        |)
        |""".stripMargin

    tableEnv.executeSql(table_jie)
    tableEnv.executeSql("insert into table_jie select * from jie")


    //------------------------------
    // 借款和还款的数据发送到下游
    //------------------------------
    val table2 = tableEnv.fromDataStream(stream1)
    tableEnv.createTemporaryView("all_data", table2)
    val table_all_data =
      """
        |create table table_all_data(
        |event_id string,
        |relation_id	string,
        |event_type	string,
        |user_id	string,
        |amt float,
        |event_dt	bigint,
        |should_repay_time	bigint,
        |repay_state	string,
        |actual_repay_time	bigint
        |)with(
        |  'connector' = 'kafka',
        |  'topic' = 'table_all_data',
        |  'properties.bootstrap.servers' = '10.1.30.8:9092',
        |  'properties.group.id' = 'dev_flink',
        |  'format' = 'json'
        |)
        |""".stripMargin
    tableEnv.executeSql(table_all_data)
    tableEnv.executeSql("insert into table_all_data select * from all_data")
  }
}

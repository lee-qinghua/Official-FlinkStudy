package com.finicalHystrix

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

object T {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    val kuduMasters = "real-time-006"
    val sql =
      """
        |create table finicalinfo(
        |relation_id	string,
        |event_type	string,
        |user_id	string,
        |amt float,
        |event_dt	bigint,
        |should_repay_time	bigint,
        |repay_state	string,
        |actual_repay_time	bigint
        |)with(
        |'connector.type'='kudu',
        |'kudu.table'='qinghua_finicalinfo',
        |'kudu.masters'='real-time-006'
        |)
        |""".stripMargin

    tableEnv.executeSql(sql)
    val table = tableEnv.sqlQuery("select count(1) from finicalinfo")

    env.execute("aaa")
  }
}

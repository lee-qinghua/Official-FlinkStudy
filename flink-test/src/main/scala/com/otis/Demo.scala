package com.otis

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

object Demo {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)

    val sql1=
      """
        |CREATE TABLE topic_products (
        |  id BIGINT,
        |  name STRING,
        |  description STRING,
        |  weight DECIMAL(10, 2)
        |) WITH (
        | 'connector' = 'kafka',
        | 'topic' = 'products_binlog',
        | 'properties.bootstrap.servers' = '10.1.30.8:9092',
        | 'properties.group.id' = 'testGroup',
        | 'scan.startup.mode' = 'latest-offset',
        | 'format' = 'canal-json'  -- using canal-json as the format
        |)
        |""".stripMargin
    tEnv.executeSql(sql1)

    tEnv.sqlQuery("select id ,sum(weight) from topic_products group by id").toRetractStream[Row].print()
    env.execute()
  }
}

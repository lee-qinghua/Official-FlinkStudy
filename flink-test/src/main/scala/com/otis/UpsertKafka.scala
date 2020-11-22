package com.otis

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
 * Test for sink data to Kafka with retract mode.
 */
object UpsertKafka {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    val data=List(
      ("1",5),
      ("1",10),
      ("2",10)
    )
    val t1 = env.fromCollection(data).toTable(tEnv).as("class", "score")
    tEnv.createTemporaryView("source_table",t1)
    val sinkDDL =
      """
        |CREATE TABLE kafkaSink (
        |id string,
        |cnt bigint)
        |with(
        |'connector' = 'kafka',
        |'topic' = 'odsTable',
        |'properties.bootstrap.servers' = '10.1.30.8:9092',
        |'properties.group.id' = 'topic.group1',
        |'format' = 'changelog-json',
        |'scan.startup.mode' = 'latest-offset')
        |""".stripMargin
    tEnv.executeSql(sinkDDL)

    val sql = "INSERT INTO kafkaSink SELECT class, SUM(score) FROM source_table GROUP BY class"
    tEnv.executeSql(sql)

    tEnv.sqlQuery("select * from kafkaSink").toRetractStream[Row].print()
    env.execute("RetractKafka")
  }
}

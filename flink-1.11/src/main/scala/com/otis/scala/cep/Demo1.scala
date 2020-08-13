package com.otis.scala.cep

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

object Demo1 {

  case class Ticker1(symbol: String, price: Long, tax: Long, rowtime: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val settings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tableEnv = StreamTableEnvironment.create(env, settings)

    val dataStream = env.socketTextStream("hadoop101", 7777)
      .map(x => {
        val arr = x.split(",")
        Ticker1(arr(0), arr(1).toLong, arr(2).toLong, arr(3).toLong * 1000)
      })
      .assignAscendingTimestamps(_.rowtime)

    //把流转换成表，这一步 指定rowtime
    val table = tableEnv.fromDataStream(dataStream, 'symbol, 'price, 'tax, 'rowtime.rowtime())

    //基于表创建视图
    tableEnv.createTemporaryView("Ticker", table)

    //注册一个打印测试表
    val printTable =
      """ CREATE TABLE print_table (
        |   c1 STRING,
        |   c2 TIMESTAMP(3),
        |   c3 TIMESTAMP(3),
        |   c4 BIGINT
        | ) WITH (
        |   'connector' = 'print'
        | )""".stripMargin
    tableEnv.executeSql(printTable)

    //============
    //  CEP 复杂时间处理
    //============

val sql =
  """
    |insert into print_table
    |select *
    |from Ticker
    |match_recognize(
    |partition by symbol
    |order by rowtime
    |measures
    |   first(A.rowtime) as start_time,
    |   last(A.rowtime) as last_time,
    |   avg(A.price) as avg_price
    |one row per match
    |after match skip past last row
    |pattern (A B)
    |define
    |   A as avg(A.price) < 15
    |) mr
    |""".stripMargin

    //目的是平均值<15 A+ 是贪婪匹配吧，就是一次或多次匹配。
    //匹配成功后不会立即输出，当有下 下 一个不匹配的元素出现时才会输出
    val s =""" insert into print_table SELECT *
             |FROM Ticker
             |    MATCH_RECOGNIZE (
             |        PARTITION BY symbol
             |        ORDER BY rowtime
             |        MEASURES
             |            FIRST(A.rowtime) AS start_tstamp,
             |            LAST(A.rowtime) AS end_tstamp,
             |            AVG(A.price) AS avgPrice
             |        ONE ROW PER MATCH
             |        AFTER MATCH SKIP PAST LAST ROW
             |        PATTERN (A+ B)
             |        DEFINE
             |            A AS AVG(A.price) < 15
             |    ) MR""".stripMargin
    tableEnv.executeSql(s)
  }
}


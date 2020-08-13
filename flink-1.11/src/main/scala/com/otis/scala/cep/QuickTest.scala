package com.otis.scala.cep

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

object QuickTest {
  //测试数据
  //ACME,10,1,1
  //ACME,11,2,2
  //ACME,12,1,3
  //ACME,13,3,4
  //ACME,14,2,5
  //ACME,15,1,6
  //ACME,16,1,7
  case class Ticker(symbol: String, price: Long, tax: Long, rowtime: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val settings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tableEnv = StreamTableEnvironment.create(env, settings)

    val dataStream = env.socketTextStream("hadoop101", 7777)
      .map(x => {
        val arr = x.split(",")
        Ticker(arr(0), arr(1).toLong, arr(2).toLong, arr(3).toLong * 1000)
      })
      .assignAscendingTimestamps(_.rowtime)

    //把流转换成表，这一步 指定rowtime
    val table = tableEnv.fromDataStream(dataStream, 'symbol, 'price, 'tax, 'rowtime.rowtime())

    //基于表创建视图
    tableEnv.createTemporaryView("Ticker", table)

    //注册一个打印测试表
//    val printTable1 =
//      """ CREATE TABLE print_table1 (
//        |   symbol STRING,
//        |   price BIGINT,
//        |   tax BIGINT,
//        |   ts TIMESTAMP(3)
//        | ) WITH (
//        |   'connector' = 'print'
//        | )""".stripMargin
//    tableEnv.executeSql(printTable1)


    val printTable4 =
      """ CREATE TABLE print_table4 (
        |   c1 STRING,
        |   c4 BIGINT
        | ) WITH (
        |   'connector' = 'print'
        | )""".stripMargin
    tableEnv.executeSql(printTable4)

//    val printTable3 =
//      """ CREATE TABLE print_table3 (
//        |    c1 STRING,
//        |   c2 TIMESTAMP(3),
//        |   c3 TIMESTAMP(3),
//        |   c4 BIGINT
//        | ) WITH (
//        |   'connector.type' = 'kafka',
//        |    'connector.version' = 'universal',
//        |    'connector.topic' = 'print_table3',
//        |      'connector.properties.zookeeper.connect' = '10.1.30.8:2181',
//        |      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
//        |      'format.type' = 'json',
//        |      'update-mode' = 'append'
//        | )""".stripMargin
//    tableEnv.executeSql(printTable3)

//    val kafkaTable =
//      """ CREATE TABLE kftable (
//        |   c1 STRING,
//        |   c2 BIGINT,
//        |   c3 BIGINT,
//        |   c4 BIGINT
//        | ) WITH (
//        |   'connector.type' = 'kafka',
//        |    'connector.version' = 'universal',
//        |    'connector.topic' = 'otis_table',
//        |      'connector.properties.zookeeper.connect' = '10.1.30.8:2181',
//        |      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
//        |      'format.type' = 'json',
//        |      'update-mode' = 'append'
//        | )""".stripMargin
//    tableEnv.executeSql(kafkaTable)

    //============
    //  CEP 复杂时间处理
    //============

    val t4 =""" insert into print_table4
              |SELECT * FROM Ticker
              |    MATCH_RECOGNIZE(
              |        PARTITION BY symbol
              |        ORDER BY rowtime
              |        MEASURES
              |        A.price AS lastPrice
              |        ONE ROW PER MATCH
              |        AFTER MATCH SKIP PAST LAST ROW
              |        PATTERN (A)
              |        DEFINE
              |              A AS A.price > 10
              |    )""".stripMargin
    tableEnv.executeSql(t4)
  }
}


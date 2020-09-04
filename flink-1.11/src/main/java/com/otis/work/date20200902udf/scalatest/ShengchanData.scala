package com.otis.work.date20200902udf.scalatest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._

object ShengchanData {

  case class MyData(cardid: String, amount: Int)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    val stream = env.socketTextStream("10.1.30.10", 7777)

    val mapstream = stream.map(x => {
      val arr = x.split(",")
      MyData(arr(0), arr(1).toInt)
    })
    val table = tableEnv.fromDataStream(mapstream)
    tableEnv.createTemporaryView("mytable", table);



  }
}

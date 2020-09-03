package com.otis.work.date20200902多进多出udf.scalatest

import java.util

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

object TestMyFunction {

  case class MyValue(score: Int)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    val stream = env.socketTextStream("10.1.30.10", 7777)
      .map(x => MyValue(x.toInt))
    println(stream)
    val table = tableEnv.fromDataStream(stream, 'score)
    tableEnv.createTemporaryView("source_table", table)
    tableEnv.registerFunction("myFun", new MyFunction)

    val table1: Table = tableEnv.sqlQuery(
      """
        |select
        |SPLIT_INDEX(myFun(score), ',', 1),
        |SPLIT_INDEX(myFun(score), ',', 2),
        |myFun(score) from source_table
        |""".stripMargin)
    table1.toRetractStream[Row].print("agg temp")
    env.execute()
  }

  class Result {
    var queue: util.LinkedList[Integer] = _
    var defaultValue: Integer = _
  }


  class MyFunction extends AggregateFunction[String, Result] {
    override def getValue(result: Result): String = {
      //取出队列中的数据,计算各个指标,最后拼接成一个字符串返回
      val list = result.queue
      //当前值
      val cur_value = list.peekFirst

      //把所有的值放到一个集合中

      var sum = 0
      import scala.collection.JavaConversions._
      for (value <- list) {
        sum += value
      }
      val builder = new StringBuilder
      builder.append(sum).append(",").append(result.queue.peekFirst)
      builder.toString
    }

    override def createAccumulator(): Result = {
      val result = new Result
      result.queue = new util.LinkedList[Integer]()
      result
    }

    def accumulate(acc: Result, temp: Int): Unit = {
      if (acc.queue.size() > 10) acc.queue.pollLast()
      acc.queue.addFirst(temp)
    }


  }

}

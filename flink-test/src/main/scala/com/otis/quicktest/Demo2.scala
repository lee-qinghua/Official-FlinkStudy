package com.otis.quicktest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
import org.junit.Test

import scala.collection.mutable

class Demo2 {
  val settings = EnvironmentSettings.newInstance().useBlinkPlanner().build()
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val tEnv = StreamTableEnvironment.create(env, settings)

  @Test
  def testnvl: Unit = {
    val data = List(
      (1, "2", "Hello2"),
      (1, "1", "Hello"),
      (2, "2", "Hello"),
      (3, "2", "Hello"),
      (4, "4", "Hello"),
      (5, "5", "Hello"),
      (5, "6", "Hello"),
      (6, "6", "Hello"),
      (7, "7", "Hello World"),
      (7, "8", "Hello World"))

    val t = env.fromCollection(data).toTable(tEnv).as("a", "b", "c")
    tEnv.createTemporaryView("MyTable", t)

    tEnv.createFunction("flink_nvl",classOf[FlinkNVLFunction])

    val sqlquery="select flink_nvl(null,3.56),flink_nvl(b,5) from MyTable"
    //val sqlquery="select a,b,c from MyTable"

    tEnv.sqlQuery(sqlquery).toAppendStream[Row].print()
    env.execute()
  }
}

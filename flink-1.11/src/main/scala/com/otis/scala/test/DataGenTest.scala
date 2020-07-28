package com.otis.scala.test

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

object DataGenTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val settings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tableEnv = StreamTableEnvironment.create(env, settings)

    //生成数据
    val dataGenStr = " CREATE TABLE source_table (" +
      " id INT," +
      " score INT," +
      " address STRING," +
      " ts AS localtimestamp," +
      " WATERMARK FOR ts AS ts" +
      " ) WITH (" +
      "'connector' = 'datagen'," +
      "'rows-per-second'='5'," +
      "'fields.id.kind'='sequence'," +
      "'fields.id.start'='1'," +
      "'fields.id.end'='100'," +
      "'fields.score.min'='1'," +
      "'fields.score.max'='100'," +
      "'fields.address.length'='10'" +
      ")"

    //print table
    val print_table= " CREATE TABLE print_table (" +
      "         id INT," +
      "         score INT," +
      "        address STRING" +
      "        ) WITH (" +
      "          'connector' = 'print'" +
      "        )"



    tableEnv.executeSql(dataGenStr)
    tableEnv.executeSql(print_table)
    tableEnv.executeSql("insert into print_table select id,score,address from source_table")
  }
}

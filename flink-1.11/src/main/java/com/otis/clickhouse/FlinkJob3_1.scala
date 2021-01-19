package com.otis.clickhouse

import java.util

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object FlinkJob3_1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val settings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tableEnv = StreamTableEnvironment.create(env, settings)

    val datagen =
      """
        |create table datagen(
        |name string,
        |address string,
        |age int
        |)with(
        |'connector'='datagen',
        |'rows-per-second'='1'
        |)
        |""".stripMargin

    tableEnv.executeSql(datagen)
    val table = tableEnv.sqlQuery("select name,address,abs(age) from datagen")
    val stream = tableEnv
      .toAppendStream[Row](table)
      .countWindowAll(10)
      .trigger(new MyTimeTrigger(5))
      .process(new MyPWFunction())


    val sql = "INSERT INTO user2 (name, address, age) VALUES (?,?,?)"
    val tableColums = Array("name", "address", "age")
    val types = Array("string", "string", "int")
    stream.print()
    stream.addSink(new MyClickHouseSink3(sql, tableColums, types))
    env.execute("clickhouse sink test")
  }

  //触发器触发或者到时间后，把所有的结果收集到了这里，在这里计算
  class MyPWFunction extends ProcessAllWindowFunction[Row, util.List[Row], GlobalWindow] {
    override def process(context: Context, elements: Iterable[Row], out: Collector[util.List[Row]]): Unit = {
      val list = new util.ArrayList[Row]
      elements.foreach(x => list.add(x))
      out.collect(list)
    }
  }

  class MyTimeTrigger(maxTime: Long) extends Trigger[Row, GlobalWindow] {
    private lazy val my_timer: ValueStateDescriptor[Long] = new ValueStateDescriptor[Long]("counter", classOf[Long])

    override def onElement(element: Row, timestamp: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      val time_state = ctx.getPartitionedState(my_timer)
      val time = time_state.value()
      val processingtime = ctx.getCurrentProcessingTime
      if (time == 0) { //首次进入数据，注册一个定时器
        ctx.registerProcessingTimeTimer(processingtime + maxTime * 1000)
        time_state.update(processingtime + maxTime * 1000)
        TriggerResult.CONTINUE
      } else if (processingtime >= time) {
        time_state.update(processingtime + maxTime * 1000)
        ctx.registerProcessingTimeTimer(processingtime + maxTime * 1000)
        TriggerResult.FIRE_AND_PURGE
      } else {
        TriggerResult.CONTINUE
      }
    }

    override def onProcessingTime(time: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE


    override def onEventTime(time: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def clear(window: GlobalWindow, ctx: Trigger.TriggerContext): Unit = {
      ctx.getPartitionedState(my_timer).clear()
      ctx.deleteProcessingTimeTimer(ctx.getPartitionedState(my_timer).value())
      TriggerResult.PURGE
    }

  }

}

package com.otis.借款熔断

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.util.Collector

object C_Core {

  case class OdsData(id: String, flag: String, event_time: Long)

  // flag 还款的状态0未还款 1还款
  // stat 0提前还款  1 逾期还款
  case class Output(id: String, flag: String, stat: String)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)


    val table_all_data =
      """
        |create table table_all_data(
        |id string,
        |flag string,
        |event_time bigint
        |)with(
        |  'connector' = 'kafka',
        |  'topic' = 'table_all_data',
        |  'properties.bootstrap.servers' = '10.1.30.8:9092',
        |  'properties.group.id' = 'dev_flink',
        |  'scan.startup.mode' = 'latest-offset',
        |  'format' = 'json'
        |)
        |""".stripMargin

    tableEnv.executeSql(table_all_data)

    // 经过处理输出每条业务的还款结果
    val stream: DataStream[Output] = tableEnv
      .sqlQuery("select * from  table_all_data")
      .toAppendStream[OdsData]
      .keyBy(_.id)
      .process(new CustomProcessFunction(60 * 1000))

    stream.print()
    //todo 实时更新kudu数据库

    env.execute("core job")

  }

  class CustomProcessFunction(timeInterval: Long) extends KeyedProcessFunction[String, OdsData, Output] {
    lazy val timerTs = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerTs", classOf[Long]))

    // 给每个key定义一个计时器，90天之后触发输出一条未还款的信息
    // 如果90天之内有还款的数据，就取消定时器
    override def processElement(value: OdsData, ctx: KeyedProcessFunction[String, OdsData, Output]#Context, out: Collector[Output]): Unit = {
      val timer = timerTs.value()

      if (value.flag == "0") { // 借款数据 第一次进来，注册定时器。
        ctx.timerService().registerProcessingTimeTimer(value.event_time * 1000 + timeInterval)
        timerTs.update(value.event_time * 1000 + timeInterval)
        println(value.event_time * 1000 + timeInterval)

      } else if (value.flag == "1" && value.event_time * 1000 < timer) { // 还款数据进来，如果当前时间小于定时器的时间，那就取消定时器就不会输出逾期的信息
        ctx.timerService().deleteProcessingTimeTimer(timer)
        timerTs.clear()
        print("id为: " + value.id + "提前完成还款！")
        out.collect(Output(value.id, "1", "0"))

      } else { // 如果还款数据进来发现没有注册定时器，那肯定是逾期之后的还款数据，直接输出还款完成的信息。
        print("id为: " + value.id + "  逾期完成还款！")
        out.collect(Output(value.id, "1", "1"))
      }
    }

    // 定时器触发
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, OdsData, Output]#OnTimerContext, out: Collector[Output]): Unit = {
      val key = ctx.getCurrentKey
      print("id为: " + key + "  未完成还款！")
      out.collect(Output(key, "0", "1"))
      timerTs.clear()
    }
  }

}

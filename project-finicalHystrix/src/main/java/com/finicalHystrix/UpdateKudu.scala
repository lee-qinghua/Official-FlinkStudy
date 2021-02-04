package com.finicalHystrix

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._

/**
 * 1. kafka中的消息有两种 1.借钱的消息 2.还钱完成的消息
 * 2. 逾期自己监控
 * 3. 逾期或者还款完成计算逾期百分比，存入redis（不是每次借钱之前访问的话那就不需要更新redis）  还款完成也要更新因为分子变小。逾期也要更新，分子变大。
 * 4. 当百分比大于10的时候，触发熔断
 */
object UpdateKudu {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 传过来的只有id flag(0借款/1还款) event_time
    val stream = env.socketTextStream("10.1.30.10", 7777)

//    val stream1 = stream.map(x => {
//      val arr = x.split(" ")
//      val id = arr(0)
//      val flag = arr(1)
//      val event_time = arr(2)
//      OdsData(id, flag, event_time)
//    })
  }
}

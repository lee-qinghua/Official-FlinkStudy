package com.streamapi.code.订单超时

import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map


object Demo2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val LoginEvent1Stream = env.fromCollection(List(
      OrderEvent1(1, "create", 1558430842),
      OrderEvent1(2, "create", 1558430843),
      OrderEvent1(2, "pay", 1558430844)
    ))
      .assignAscendingTimestamps(_.ts * 1000)
      .keyBy(_.orderId)

    //声明一个pattern
    val myPattern: Pattern[OrderEvent1, OrderEvent1] = Pattern.begin[OrderEvent1]("begin").where(_.orderState == "create")
      .followedBy("follow").where(_.orderState == "pay")
      .within(Time.minutes(15))
    //定义一个侧输出流Tag 让匹配不成功的流去侧输出流
    val outTag: OutputTag[OrderResult1] = OutputTag[OrderResult1]("chaoshi")

    val unit: PatternStream[OrderEvent1] = CEP.pattern(LoginEvent1Stream, myPattern)

    val unit1: DataStream[OrderResult1] = unit.select(outTag)(
      (patternTimeoutFunction: Map[String, Iterable[OrderEvent1]], ts: Long) => {
        val id: Int = patternTimeoutFunction.getOrElse("begin", null).iterator.next().orderId
        OrderResult1(id, "超时")
      }
    )(
      (pattern: Map[String, Iterable[OrderEvent1]]) => {
        val id: Int = pattern.getOrElse("begin", null).iterator.next().orderId
        OrderResult1(id, "成功")
      }
    )
    unit1.print("success")
    unit1.getSideOutput(outTag).print("fail")
    env.execute()
  }

  case class OrderResult1(orderId: Int, orderState: String) {}

  case class OrderEvent1(orderId: Int, orderState: String, ts: Long) {}

}
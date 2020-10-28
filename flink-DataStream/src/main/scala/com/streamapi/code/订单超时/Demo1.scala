package com.streamapi.code.订单超时

import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object Demo1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val LoginEvent1Stream = env.fromCollection(List(
      OrderEvent(1, "create", 1558430842),
      OrderEvent(2, "create", 1558430843),
      OrderEvent(2, "pay", 1558430844)
    ))
      .assignAscendingTimestamps(_.ts * 1000)
      .keyBy(_.orderId)

    val myPattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin").where(_.orderState == "create")
      .followedBy("next").where(_.orderState == "pay").within(Time.minutes(15))

    val patternStream: PatternStream[OrderEvent] = CEP.pattern(LoginEvent1Stream, myPattern)
    //定义一个侧输出流的tag 模式不匹配成功的时候就走这个流
    val chaoshiTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("chaoshi")


    import scala.collection.Map
    val unit: DataStream[OrderResult] = patternStream.select(chaoshiTag)(
      (pattern: Map[String, Iterable[OrderEvent]], ts: Long) => {
        val id: Int = pattern.getOrElse("begin", null).iterator.next().orderId
        OrderResult(id, "chaoshi")
      }
    )(
      (pattern: Map[String, Iterable[OrderEvent]]) => {
        val id: Int = pattern.getOrElse("next", null).iterator.next().orderId
        OrderResult(id, "sucess")
      }
    )
    unit.print("success") //匹配的流
    val value: DataStream[OrderResult] = unit.getSideOutput(chaoshiTag)
    value.print("fail") //未匹配的流
    env.execute()
  }

  case class OrderResult(orderId: Int, orderState: String) {}

  case class OrderEvent(orderId: Int, orderState: String, ts: Long) {}

}
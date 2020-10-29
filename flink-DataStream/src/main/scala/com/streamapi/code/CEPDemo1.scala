//package com.streamapi.code
//
//import org.apache.flink.cep.scala.CEP
//import org.apache.flink.cep.scala.pattern.Pattern
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.windowing.time.Time
//
//case class LoginEvent1(id: Long, ip: String, result: String, ts: Long)
//
//object CEPDemo1 {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.setParallelism(1)
//    val LoginEvent1Stream = env.fromCollection(List(
//      LoginEvent1(1, "192.168.0.1", "fail", 1558430842),
//      LoginEvent1(1, "192.168.0.2", "fail", 1558430843),
//      LoginEvent1(1, "192.168.0.3", "fail", 1558430844),
//      LoginEvent1(1, "192.168.0.3", "fail", 1558430845),
//      LoginEvent1(2, "192.168.0.3", "fail", 1558430845),
//      LoginEvent1(2, "192.168.10.10", "success", 1558430845)
//    ))
//      .assignAscendingTimestamps(_.ts * 1000)
//      .keyBy(_.id)
//
//    //定义一个匹配模式
//    val loginFailPattern = Pattern.begin[LoginEvent1]("begin").where(_.result == "fail")
//      .next("next").where(_.result == "fail")
//      .times(2)//是说的"next"两次
//      .within(Time.seconds(4))
//
//    val unit1 = CEP.pattern(LoginEvent1Stream, loginFailPattern)
//
//    //todo 这里的map是这个类型的
//    import scala.collection.Map
//    val value: DataStream[(Long, String, Long)] = unit1.select(
//      //Map[String,Iterable[LoginEvent1]] string 就是我们上面定义的name(begin next)
//      (x: Map[String, Iterable[LoginEvent1]]) => {
//        val event: LoginEvent1 = x.getOrElse("next", null).iterator.next()
//        (event.id, event.result, event.ts)
//      })
//    value.print()
//    env.execute()
//  }
//}

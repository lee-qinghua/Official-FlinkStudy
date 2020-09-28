package com.otis.scala.company

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object 自动根据表名生成代码 {


  case class Info(id: String)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[String] = env.socketTextStream("10.1.30.10", 7777)
    stream
      .map(x => {
        Info(x)
      })
      .keyBy(_.id)
      .timeWindow(Time.seconds(10))
      .process(new MyProFunction2)
      .print()

    env.execute("relation")
  }


  class MyProFunction2 extends ProcessWindowFunction[Info, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Info], out: Collector[String]): Unit = {
      val list = elements.toList
      val builder = new StringBuilder
      builder.append(" //===========================================================================================================================================").append("\n")
      builder.append("//                                                          todo ").append(key).append("\n")
      builder.append(" //===========================================================================================================================================").append("\n")
      builder.append("val ").append(key).append("=\n").append("createView(tableEnv,").append(key).append(",\"").append(key).append("\")").append("\n")
      builder.append("//    tableEnv.sqlQuery(\"select * from ").append(key).append("\").toAppendStream[Row].print()").append("\n")
      out.collect(builder.toString())
    }
  }





}

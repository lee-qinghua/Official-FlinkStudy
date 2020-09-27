package com.otis.scala.company

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object 自动格式化代码 {

  // 就业状况 PB040D01 [1..1] 无
  // 工作单位 PB040Q01 [1..1] 无
  // 单位性质 PB040D02 [1..1] 无
  // 行业 PB040D03 [1..1] 无
  // 单位地址 PB040Q02 [1..1] 无
  // 单位电话 PB040Q03 [1..1] 无
  // 职业 PB040D04 [1..1] 无
  // 职务 PB040D05 [1..1] 无
  // 职称 PB040D06 [1..1] 无
  // 进入本单位年份 PB040R01 [1..1] 无
  // 信息更新日期 PB040R02 [1..1] 无
  case class Data(id: String, number: String, matchField: String, compareMethod: String, keywordType: String, keyword: String, code: String)

  case class Info(id: String, a: String, b: String, c: String, d: String)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[String] = env.socketTextStream("10.1.30.10", 7777)
    stream
      .map(x => {
        val arr = x.split(" ")
        Info("1", arr(0), arr(1), arr(2), arr(3))
      })
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .process(new MyProFunction1)
      .print()

    env.execute("relation")
  }

  class MyProFunction1 extends ProcessWindowFunction[Info, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Info], out: Collector[String]): Unit = {
      val list = elements.toList
      val builder = new StringBuilder
      for (i <- list.indices) {
        if (i == 0) {
          builder.append(list(i).b).append(" STRING")
        } else {
          builder.append(",").append(list(i).b).append(" STRING")
        }
      }
      out.collect(builder.toString())
    }
  }



}

package com.otis.scala.company

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//  PA01C ROW(PA01CS01 STRING,PA01CH ARRAY<ROW(PA01CD01 STRING,PA01CI01 STRING)>)
//  只是解析string类型的 不解析array类型
object 自动生成sql语句 {

  case class Info(id: String)

  //PRH.PA01.PA01B ROW(PA01BQ01 STRING,PA01BD01 STRING,PA01BI01 STRING,PA01BI02 STRING,PA01BD02 STRING)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[String] = env.socketTextStream("10.1.30.10", 7777)
    stream
      .map(str => {
        val id = str.split("ROW")(0).replace(" ", "")
        val data = str.split("ROW")(1)
          .replace("(", "")
          .replace(")", "")
          .split(",")
        for (elem <- data.toList) {
          val tag = elem.split(" ")(0)
          val mtype = elem.split(" ")(1)
          if (mtype.equals("STRING")) {
            println(id + "." + tag)
          }
        }
      })
//      .keyBy(_.id)
//      .timeWindow(Time.seconds(10))
//      .process(new MyProFunction2)
//      .print()

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
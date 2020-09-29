package com.otis.scala.company

import java.util

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//  PA01C ROW(PA01CS01 STRING,PA01CH ARRAY<ROW(PA01CD01 STRING,PA01CI01 STRING)>)
//  只是解析string类型的 不解析array类型
object array自动生成step1 {

  //PB03 ARRAY<ROW(PB030D01 STRING,PB030Q01 STRING,PB030Q02 STRING,PB030R01 STRING)>)
  //id    =>  PB03
  //data  =>  PB030D01 STRING,PB030Q01 STRING,PB030Q02 STRING,PB030R01 STRING
  case class Info(id: String, data: String)

  //PRH.PA01.PA01B ROW(PA01BQ01 STRING,PA01BD01 STRING,PA01BI01 STRING,PA01BI02 STRING,PA01BD02 STRING)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[String] = env.socketTextStream("10.1.30.10", 7777)
    stream
      .map(str => {
        val id1 = str.split("ARRAY")(0).split("ROW")(0).replace(" ", "")
        val str2 = str.split("ARRAY")(0).replace(" ", "")
        val constr = str2.substring(str2.length - 7)
        var id2 = ""
        if (constr.contains(",")) {
          val arr = str2.split(",")
          val length = arr.length
          id2 = arr(length - 1)
        } else {
          val arr = str2.split("\\(")
          val length = arr.length
          id2 = arr(length - 1)
        }
        var id = ""
        if (id1 == id2) {
          id = id1
        } else {
          id = id1 + "." + id2
        }
        val data = str.split("ARRAY")(1).split("ROW")(1).replace("(", "").replace(")", "").replace(">", "")
        Info(id, data)
      })
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .process(new MyProFunction2)
      .print()

    env.execute("relation")
  }

  class MyProFunction2 extends ProcessWindowFunction[Info, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Info], out: Collector[String]): Unit = {
      val head: Info = elements.head
      val data = head.data
      val id = head.id
      //生成代码
      val builder = new StringBuilder
      //把data作为key 查询的时候判断 key.contains（xxx）

      val arr = data.split(",")
      builder.append("select \n").append("PRH.PA01.PA01A.PA01AI01  \t\t\t\tas report_id,\n")
        .append(id).append("         as data,\n")
        .append("PRH.PA01.PA01A.PA01AI01  \t\t\t\tas SID,\n")
        .append("'2020-09-27'                            as STATISTICS_DT\n")
        .append(")t1,unnest(t1.data) as info(")
      val list = arr.toList
      for (i <- list.indices) {
        val tag = list(i).split(" ")(0)
        if (i == list.size - 1) {
          builder.append(tag)
        } else {
          builder.append(tag).append(",")
        }
      }
      builder.append(")\n")
      out.collect(builder.toString())
    }
  }

}
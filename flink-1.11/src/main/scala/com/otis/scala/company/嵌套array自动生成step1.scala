package com.otis.scala.company

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object 嵌套array自动生成step1 {

  case class Info(table_name: String, data: String, tags: String)

  //PAHPF08-PF08Z ROW(PF08ZS01 STRING,PF08ZH ARRAY<ROW(PF08ZD01 STRING,PF08ZQ01 STRING,PF08ZR01 STRING)>)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[String] = env.socketTextStream("10.1.30.10", 7777)
    stream
      .map(str => {
        val table_name = str.split("-")(0)
        val id1 = str.split("-")(1).split(" ")(0)
        val arr = str.split("-")(1).split(" ARRAY")(0).split(",")
        val id2 = arr(arr.length - 1)
        val data = id1 + "." + id2
        val tags = str.split("ARRAY")(1).split("ROW")(1).replace("(", "").replace(">", "").replace(")", "")
        Info(table_name, data, tags)
      })
      .keyBy(_.data)
      .timeWindow(Time.seconds(5))
      .process(new MyProFunction2)
      .print()

    env.execute("relation")
  }

  class MyProFunction2 extends ProcessWindowFunction[Info, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Info], out: Collector[String]): Unit = {
      val head = elements.head
      val builder = new StringBuilder
      builder.append("from(\nselect\nreport_id \t\t\tas report_id,\nSID \t\t\t\tas SID,\nSTATISTICS_DT \t\tas STATISTICS_DT,\n")
        .append(head.data).append("     as data\nfrom ").append(head.table_name).append("\n")
        .append(")t1,unnest(t1.data) as info(")
      val list = head.tags.split(",").toList
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
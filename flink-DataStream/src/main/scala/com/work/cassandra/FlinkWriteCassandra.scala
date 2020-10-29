package com.work.cassandra

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.cassandra.CassandraSink

/**
 * 实时写入cassandra数据库
 */
object FlinkWriteCassandra {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val text: DataStream[String] = env.socketTextStream("10.1.30.10", 7777)

    // parse the data, group it, window it, and aggregate the counts
    val result: DataStream[(String, Int)] = text
      // split up the lines in pairs (2-tuples) containing: (word,1)
      .flatMap(_.toLowerCase.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      // group by the tuple field "0" and sum up tuple field "1"
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .sum(1)

    CassandraSink.addSink(result)
      .setQuery("INSERT INTO flink.wc(word, count) values (?, ?);")
      .setHost("10.1.30.10")
      .build()

    result.print().setParallelism(1)
    env.execute()
  }
}

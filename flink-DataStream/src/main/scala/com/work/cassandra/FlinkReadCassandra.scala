package com.work.cassandra

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

/**
 * 实时读取cassandra数据库
 * 传进来的数据为id,name,age 根据id查询数据库中score 拼接在一起返回完整的信息
 */
object FlinkReadCassandra {

  case class Stu(id: String, name: String, age: Int)

  case class Student(id: String, name: String, age: Int, score: Int)

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val text: DataStream[String] = env.socketTextStream("10.1.30.10", 7777)

    val result = text
      .map(x => {
        val arr = x.split(",")
        Stu(arr(0), arr(1), arr(2).toInt)
      })
      .flatMap(new ReadCassandraFlatMap())
    result.print().setParallelism(1)

    env.execute()
  }
}

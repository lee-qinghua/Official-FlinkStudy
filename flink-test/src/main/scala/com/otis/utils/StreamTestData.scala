package com.otis.utils

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import scala.collection.mutable

object StreamTestData {
  def getSingletonDataStream(env: StreamExecutionEnvironment): DataStream[(Int, Long, String)] = {
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 22L, "Hi"))
    env.fromCollection(data)
  }

  def getSmall3TupleDataStream(env: StreamExecutionEnvironment): DataStream[(Int, Long, String)] = {
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "Hi"))
    data.+=((2, 2L, "Hello"))
    data.+=((3, 2L, "Hello world"))
    env.fromCollection(data)
  }

  def get3TupleDataStream(env: StreamExecutionEnvironment): DataStream[(Int, Long, String)] = {
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "Hi"))
    data.+=((2, 2L, "Hello"))
    data.+=((3, 2L, "Hello world"))
    data.+=((4, 3L, "Hello world, how are you?"))
    data.+=((5, 3L, "I am fine."))
    data.+=((6, 3L, "Luke Skywalker"))
    data.+=((7, 4L, "Comment#1"))
    data.+=((8, 4L, "Comment#2"))
    data.+=((9, 4L, "Comment#3"))
    data.+=((10, 4L, "Comment#4"))
    data.+=((11, 5L, "Comment#5"))
    data.+=((12, 5L, "Comment#6"))
    data.+=((13, 5L, "Comment#7"))
    data.+=((14, 5L, "Comment#8"))
    data.+=((15, 5L, "Comment#9"))
    data.+=((16, 6L, "Comment#10"))
    data.+=((17, 6L, "Comment#11"))
    data.+=((18, 6L, "Comment#12"))
    data.+=((19, 6L, "Comment#13"))
    data.+=((20, 6L, "Comment#14"))
    data.+=((21, 6L, "Comment#15"))
    env.fromCollection(data)
  }
}

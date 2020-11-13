package com.testcheckpoint

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._

object RecoverDemo {

  case class Chengji(id: String, score: Int, ts: Long)

  def main(args: Array[String]): Unit = {
    // 创建一个env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    // 5s 一次checkpoint
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    // 确认 checkpoints 之间的时间会进行 500 ms
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)

    // Checkpoint 必须在一分钟内完成，否则就会被抛弃
    env.getCheckpointConfig.setCheckpointTimeout(60000)

    // 如果 task 的 checkpoint 发生错误，会阻止 task 失败，checkpoint 仅仅会被抛弃
    //    env.getCheckpointConfig.setFailTasksOnCheckpointingErrors(false)

    // 同一时间只允许一个 checkpoint 进行
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.setStateBackend(new FsStateBackend("state.checkpoints.dir: hdfs://real-time-001:8020/user/flink/test-checkpoint-dir"))


    val value = env.socketTextStream("10.1.30.10", 7777)
      .map(x => {
        val arr = x.split(" ")
        Chengji(arr(0), arr(1).toInt, arr(2).toLong * 1000)
      })
      .uid("map1")
      .assignAscendingTimestamps(_.ts)
      .keyBy(_.id)
      .sum("score")
      .uid("sum1")
      .print().setParallelism(1)

    env.execute("aaa")
  }


}

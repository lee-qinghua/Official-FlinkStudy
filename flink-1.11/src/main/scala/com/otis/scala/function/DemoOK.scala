package com.otis.scala.function

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

object DemoOK {

  case class Stu(id: String, age: Int, ts: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val settings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tableEnv = StreamTableEnvironment.create(env, settings)

    val dataStream = env.socketTextStream("hadoop101", 7777)
      .map(x => {
        val arr = x.split(",")
        Stu(arr(0), arr(1).toInt, arr(2).toLong * 1000)
      })
      .assignAscendingTimestamps(_.ts)

    //把流转换成表，这一步 指定rowtime
    val table = tableEnv.fromDataStream(dataStream, 'id, 'age, 'ts, 'rt.rowtime())

    //基于表创建视图
    tableEnv.createTemporaryView("mytable", table)

    //创建函数
    tableEnv.createFunction("myFunction2", classOf[AvgTemp])

    //单纯的测试表中有没有数据
    //    val table2 = tableEnv.sqlQuery("select * from mytable")
    //    table2.toAppendStream[Row].print("all")

    //测试函数
    val table1 = tableEnv.sqlQuery("select id,myFunction2(cast (age as double)) as hehe from mytable group by id"
    )
    table1.toRetractStream[Row].print()

    env.execute("table job")
  }

  //定义一个中间结果的样例类，用于保存聚合状态
  class AvgAcc {
    var sum: Double = 0.0
    var count: Int = 0
  }

  //定义一个聚合函数
  class AvgTemp extends AggregateFunction[Double, AvgAcc] {
    override def getValue(acc: AvgAcc): Double = acc.sum / acc.count

    override def createAccumulator(): AvgAcc = new AvgAcc

    //每来一个元素的计算方法
    def accumulate(acc: AvgAcc, temp: Double): Unit = {
      System.out.println("进入计算方法！！！")
      acc.sum += temp
      acc.count += 1
    }
  }

}

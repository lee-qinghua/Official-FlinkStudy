package com.otis.scala.function

import java.util

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row


object LagFunctionTest {

  case class Stu(id: String, age: Int, ts: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val settings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tableEnv = StreamTableEnvironment.create(env, settings)

    val dataStream = env.socketTextStream("10.1.30.10", 7777)
      .map(x => {
        val arr = x.split(",")
        Stu(arr(0), arr(1).toInt, arr(2).toLong * 1000)
      })
      .assignAscendingTimestamps(_.ts)

    //创建表
    val table = tableEnv.fromDataStream(dataStream, 'id, 'age, 'ts, 'rt.rowtime())

    //创建视图
    tableEnv.createTemporaryView("mytable", table)

    //注册函数
    tableEnv.createFunction("myFunction", classOf[PreTemp])

    //执行sql
    val table2 = tableEnv.sqlQuery("select * from mytable")
    table2.toAppendStream[Row].print("all")
    //(partition by id order by rt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    val table1 = tableEnv.sqlQuery("select id,myFunction(age,2,0) over(partition by id order by rt) as hehe from mytable")


    table1.toRetractStream[Row].print()
    env.execute("table job")
  }


  //定义一个中间结果的样例类，用于保存聚合状态
  class PreAcc {
    var myList: java.util.LinkedList[Int] = new util.LinkedList[Int]()
  }

  //定义一个聚合函数
  class PreTemp extends AggregateFunction[Int, PreAcc] {
    override def getValue(accumulator: PreAcc): Int = {
      val i = accumulator.myList.removeLast()
      System.out.print("获取值之后当前队列的大小：" + accumulator.myList.size())
      i
    }

    override def createAccumulator(): PreAcc = new PreAcc

    def accumulate(accumulator: PreAcc, value: Int, length: Int, default: Int) = {
      accumulator.myList.addFirst(value)
      val size = accumulator.myList.size
      if (size < length) {
        for (i <- size until (length + 1)) {
          accumulator.myList.add(0);
        }
      } else if (size > length + 1) throw new RuntimeException("队列的大小出错了！！")
    }
  }

}

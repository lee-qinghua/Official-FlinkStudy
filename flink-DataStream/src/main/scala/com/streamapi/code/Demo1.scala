package com.streamapi.code

import java.lang
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 每隔5分钟输出一个小时之内点击量前5的商品
 */
object Demo1 {

  //定义数据源类
  case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

  // 商品点击量(窗口操作的输出类型)
  case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

  def main(args: Array[String]): Unit = {
    // 创建一个env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 显式地定义Time类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val ds: DataStream[UserBehavior] = env.readTextFile("D:\\project\\Official-FlinkStudy\\flink-DataStream\\src\\main\\scala\\com\\streamapi\\files\\UserBehavior.csv")
      .map(x => {
        val arr = x.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000) //这里为什么可以直接用asceding 因为数据的时间时排好序的
      .filter(_.behavior == "pv") //只统计浏览的行为
    val ks: KeyedStream[UserBehavior, Long] = ds.keyBy(_.itemId)

    ks.timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new Demo1AggFunction, new Demo1WindowFunction)
      .keyBy(_.windowEnd)
      .process(new Demo1KeyFunction(5)).print()
    env.execute()
  }

  //keyby 之后调用keyedProcessFunction ,可以在这方法里面定义定时器
  class Demo1KeyFunction(num: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
    // 定义状态ListState
    var itemState: ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemState", classOf[ItemViewCount]))
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      super.onTimer(timestamp, ctx, out)
      val allItems: ListBuffer[ItemViewCount] = ListBuffer() //定义一个list 把itemState中的元素全部添加到list中，排序然后取前几名
      import scala.collection.JavaConversions._
      val counts: lang.Iterable[ItemViewCount] = itemState.get()
      for (i <- counts) {
        //allItems.add(i)
        allItems += i
      }
      // 清除状态中的数据，释放空间
      itemState.clear()
      val sortedItems: ListBuffer[ItemViewCount] = allItems.sortWith((x, y) => {
        x.count > y.count
      }).take(num)
      // 将排名数据格式化，便于打印输出
      val result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")
      //for(i<- 0 until  sortedItems.size)
      for (i <- sortedItems.indices) {
        val currentItem: ItemViewCount = sortedItems(i)
        // 输出打印的格式 e.g.  No1：  商品ID=12224  浏览量=2413
        result.append("No").append(i + 1).append(":")
          .append("  商品ID=").append(currentItem.itemId)
          .append("  浏览量=").append(currentItem.count).append("\n")
      }
      result.append("====================================\n\n")
      // 控制输出频率
      Thread.sleep(1000)
      out.collect(result.toString)
    }

    override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
      //来一条数据 在itemState中增加一条，并定义一个计时器
      //每条数据都会定义一个计时器，因为触发时间都是一样的所以没关系。但是也可以定义一个state 存储有没有这个windowend 的标志，如果有就不再定义
      itemState.add(i)
      // 注册定时器，触发时间定为 windowEnd + 1，触发时说明window已经收集完成所有数据
      context.timerService().registerEventTimeTimer(i.windowEnd + 1)
    }
  }

  // WindowFunction将每个key每个窗口聚合后的结果带上其他信息进行输出
  // 自定义实现Window Function，输出ItemViewCount格式
  class Demo1WindowFunction extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
    override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      //为什么要加上窗口闭合时的时间？因为求的是在同一窗口闭合时，点击量前几的商品，之后会根据windowend 重新分组 计算
      out.collect(ItemViewCount(key, window.getEnd, input.head))
    }
  }

  //提前聚合掉数据，减少state的存储压力
  //COUNT统计的聚合函数实现，每出现一条记录就加一
  class Demo1AggFunction extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

}

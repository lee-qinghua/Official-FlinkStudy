package com.finicalHystrix


import com.finicalHystrix.A_SplitAndEtl.OdsData
import com.finicalHystrix.B_Save2Kudu.{CustomColumnSchemaFactory, CustomCreateTableOptionsFactory}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.connectors.kudu.connector.KuduTableInfo._
import org.apache.flink.connectors.kudu.connector.writer.{AbstractSingleOperationMapper, KuduWriterConfig, RowOperationMapper}
import org.apache.flink.connectors.kudu.streaming.KuduSink
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
 *
 * 1.有逾期数据的时候会计算一次当前的逾期百分比，其余情况不会计算。只会更新kudu中的数据
 *
 */
object C_Core {

  // case class OdsData(event_id: String, relation_id: String, event_type: String, user_id: String, amt: Float, event_dt: Long, should_repay_time: Long, repay_state: String, actual_repay_time: Long)
  case class Output(relation_id: String, repay_state: String, actual_repay_time: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)


    val table_all_data =
      """
        |create table table_all_data(
        |event_id string,
        |relation_id	string,
        |event_type	string,
        |user_id	string,
        |amt float,
        |event_dt	bigint,
        |should_repay_time	bigint,
        |repay_state	string,
        |actual_repay_time	bigint
        |)with(
        |  'connector' = 'kafka',
        |  'topic' = 'table_all_data',
        |  'properties.bootstrap.servers' = '10.1.30.8:9092',
        |  'properties.group.id' = 'dev_flink',
        |  'scan.startup.mode' = 'latest-offset',
        |  'format' = 'json'
        |)
        |""".stripMargin
    tableEnv.executeSql(table_all_data)

    // 经过处理输出每条业务的还款结果
    val stream: DataStream[Output] = tableEnv
      .sqlQuery("select * from  table_all_data")
      .toAppendStream[OdsData]
      .keyBy(_.relation_id)
      .process(new CustomProcessFunction())

    //stream.print()
    // todo 更新kudu
    val sql =
    """
      |create table finicalinfo(
      |relation_id	string,
      |event_type	string,
      |user_id	string,
      |amt float,
      |event_dt	bigint,
      |should_repay_time	bigint,
      |repay_state	string,
      |actual_repay_time	bigint
      |)with(
      |'connector.type'='kudu',
      |'kudu.table'='qinghua_finicalinfo',
      |'kudu.masters'='real-time-006'
      |)
      |""".stripMargin

    tableEnv.executeSql(sql)


    env.execute("core job")
  }

  /**
   * 要输出的数据：
   * 1. 提前还款 repay_state=1
   * 2. 逾期未还款  repay_state=2
   * 3. 逾期还款  repay_state=3
   *
   * 注册定时器的时间根据should_repay_time这个字段定义，因为是processing time
   *
   */
  class CustomProcessFunction extends KeyedProcessFunction[String, OdsData, Output] {
    lazy val timerTs = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerTs", classOf[Long]))

    // 给每个key定义一个计时器，90天之后触发输出一条未还款的信息
    // 如果90天之内有还款的数据，就取消定时器
    override def processElement(value: OdsData, ctx: KeyedProcessFunction[String, OdsData, Output]#Context, out: Collector[Output]): Unit = {
      val timer = timerTs.value()

      if (value.event_type == "0") { // 借款数据 第一次进来，注册定时器。
        ctx.timerService().registerProcessingTimeTimer(value.should_repay_time * 1000)
        timerTs.update(value.should_repay_time * 1000)
        println(value.should_repay_time * 1000)
      } else if (value.event_type == "1" && value.event_dt * 1000 < timer) { // 还款数据进来，如果当前时间小于定时器的时间，那就取消定时器就不会输出逾期的信息
        ctx.timerService().deleteProcessingTimeTimer(timer)
        timerTs.clear()
        print("id为: " + value.relation_id + "提前完成还款！")
        out.collect(Output(value.relation_id, "1", value.event_dt))
      } else { // 如果还款数据进来发现没有注册定时器，那肯定是逾期之后的还款数据，直接输出还款完成的信息。
        print("id为: " + value.relation_id + "  逾期完成还款！")
        out.collect(Output(value.relation_id, "3", value.event_dt))
      }
    }

    // 定时器触发
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, OdsData, Output]#OnTimerContext, out: Collector[Output]): Unit = {
      val key = ctx.getCurrentKey
      print("id为: " + key + "  未完成还款！")
      out.collect(Output(key, "2", timestamp / 1000))
      timerTs.clear()
    }
  }

}

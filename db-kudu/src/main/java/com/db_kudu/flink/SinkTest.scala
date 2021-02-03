package com.db_kudu.flink

import java.util

import com.db_kudu._
import com.db_kudu.streaming.KuduSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.shaded.com.google.common.collect.Lists
import org.apache.kudu.{ColumnSchema, Type}

/**
 * 测试ok
 */
object SinkTest {
  def main(args: Array[String]): Unit = {

    // 在本地可以访问web_ui
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.setParallelism(1)
    val settings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tableEnv = StreamTableEnvironment.create(env, settings)

    val datagen =
      """
        |create table datagen(
        |id int,
        |money int
        |)with(
        |'connector'='datagen',
        |'rows-per-second'='100'
        |)
        |""".stripMargin

    tableEnv.executeSql(datagen)
    val table = tableEnv.sqlQuery("select id,money from datagen")
    val stream = tableEnv.toAppendStream[Row](table)

    // 构建kudu sink对象
    val masterAddresses = "real-time-006"
    val tableInfo = KuduTableInfo
      .forTable("test_speed")
      .createTableIfNotExists(new CustomColumnSchemaFactory, new CustomCreateTableOptionsFactory)
    val columns = Array[String]("id", "money")

    val writerConfig: KuduWriterConfig = KuduWriterConfig.Builder.setMasters(masterAddresses).setEventualConsistency.build
    val sink = new KuduSink[Row](writerConfig, tableInfo, new RowOperationMapper(columns, AbstractSingleOperationMapper.KuduOperation.INSERT))
    stream.addSink(sink)
    env.execute("test kudu sink")
  }

  class CustomCreateTableOptionsFactory extends CreateTableOptionsFactory {
    override def getCreateTableOptions: CreateTableOptions = {
      new CreateTableOptions().setNumReplicas(1).addHashPartitions(Lists.newArrayList("id"), 6)
    }
  }

  class CustomColumnSchemaFactory extends ColumnSchemasFactory {
    override def getColumnSchemas: util.List[ColumnSchema] = {
      val c1 = new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build
      val c2 = new ColumnSchema.ColumnSchemaBuilder("money", Type.INT32).build
      val list = new util.ArrayList[ColumnSchema]()
      list.add(c1)
      list.add(c2)
      list
    }
  }

}

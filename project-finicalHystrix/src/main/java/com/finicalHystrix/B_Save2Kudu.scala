package com.finicalHystrix

import java.util

import org.apache.flink.connectors.kudu.connector.writer.{AbstractSingleOperationMapper, KuduWriterConfig, RowOperationMapper}
import org.apache.flink.connectors.kudu.connector.{ColumnSchemasFactory, CreateTableOptionsFactory, KuduTableInfo}
import org.apache.flink.connectors.kudu.streaming.KuduSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.shaded.com.google.common.collect.Lists
import org.apache.kudu.{ColumnSchema, Type}

/**
 *
 * 保存借款明细数据到kudu中
 *
 */

object B_Save2Kudu {
  // todo 存在的问题，插入相同主键的数据，程序会挂掉
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    val table_jie =
      """
        |create table table_jie(
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
        |  'topic' = 'qinghua_jie',
        |  'properties.bootstrap.servers' = '10.1.30.8:9092',
        |  'scan.startup.mode' = 'latest-offset',
        |  'properties.group.id' = 'dev_flink',
        |  'format' = 'json'
        |)
        |""".stripMargin

    tableEnv.executeSql(table_jie)
    val stream: DataStream[Row] = tableEnv.sqlQuery("select * from table_jie").toAppendStream[Row]


    // 构建kudu sink对象
    val masterAddresses = "real-time-006"
    val tableInfo = KuduTableInfo
      .forTable("qinghua_finicalinfo")
      .createTableIfNotExists(new CustomColumnSchemaFactory, new CustomCreateTableOptionsFactory)
    val columns = Array[String]("relation_id", "event_type", "user_id", "amt", "event_dt", "should_repay_time", "repay_state", "actual_repay_time")

    val writerConfig: KuduWriterConfig = KuduWriterConfig.Builder.setMasters(masterAddresses).setEventualConsistency.build
    val sink: KuduSink[Row] = new KuduSink[Row](writerConfig, tableInfo, new RowOperationMapper(columns, AbstractSingleOperationMapper.KuduOperation.INSERT))
    stream.addSink(sink)
    env.execute("test kudu sink")
  }

  class CustomCreateTableOptionsFactory extends CreateTableOptionsFactory {
    override def getCreateTableOptions: CreateTableOptions = {
      new CreateTableOptions().setNumReplicas(1).addHashPartitions(Lists.newArrayList("relation_id"), 6)
    }
  }

  class CustomColumnSchemaFactory extends ColumnSchemasFactory {
    override def getColumnSchemas: util.List[ColumnSchema] = {
      val list = new util.ArrayList[ColumnSchema]()
      list.add(new ColumnSchema.ColumnSchemaBuilder("relation_id", Type.STRING).key(true).build())
      list.add(new ColumnSchema.ColumnSchemaBuilder("event_type", Type.STRING).build())
      list.add(new ColumnSchema.ColumnSchemaBuilder("user_id", Type.STRING).build())
      list.add(new ColumnSchema.ColumnSchemaBuilder("amt", Type.DECIMAL).build())
      list.add(new ColumnSchema.ColumnSchemaBuilder("event_dt", Type.INT64).build())
      list.add(new ColumnSchema.ColumnSchemaBuilder("should_repay_time", Type.INT64).build())
      list.add(new ColumnSchema.ColumnSchemaBuilder("repay_state", Type.STRING).build())
      list.add(new ColumnSchema.ColumnSchemaBuilder("actual_repay_time", Type.INT64).build())
      list
    }
  }

}


//===================================
//创建明细表
//===================================
//    public void createTable() throws KuduException {
//        //判断表是否存在，不存在就构建
//        if (!kuduClient.tableExists(tableName)) {
//            //构建创建表的 schema 信息-----就是表的字段和类型
//            ArrayList<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();
//            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("relation_id",Type.STRING).key(true).build());
//            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("event_type",Type.STRING).build());
//            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("user_id",Type.INT32).build());
//            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("amt",Type.DECIMAL).build());
//            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("event_dt",Type.INT64).build());
//            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("should_repay_time",Type.INT64).build());
//            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("repay_state",Type.STRING).build());
//            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("actual_repay_time",Type.INT64).build());
//            Schema schema = new Schema(columnSchemas);
//            //指定创建表的相关属性
//            CreateTableOptions options = new CreateTableOptions();
//            ArrayList<String> partitionList = new ArrayList<String>();
//            //指定 kudu 表的分区字段是什么
//            partitionList.add("relation_id"); // 按照 id.hashcode % 分区数 = 分区号
//            options.addHashPartitions(partitionList, 6);
//            kuduClient.createTable(tableName, schema, options);
//        }
//    }
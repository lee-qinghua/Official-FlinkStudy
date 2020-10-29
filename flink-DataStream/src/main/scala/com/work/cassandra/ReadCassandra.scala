package com.work.cassandra

import com.datastax.driver.core.Cluster
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder
import org.apache.flink.util.Collector

/**
 * 第一个string 是传进来的数据 比如需要查询的id
 * 第二个（string,string） 是输出数据的格式
 */
class ReadCassandra extends RichFlatMapFunction[String, (String, String)] {
  var cassandraBuilder: ClusterBuilder = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // open cassandra connection
    cassandraBuilder = new ClusterBuilder {
      override def buildCluster(builder: Cluster.Builder): Cluster = {
        builder.addContactPoint("10.1.30.10").withPort(9042).build()
      }
    }
  }

  override def flatMap(key: String, out: Collector[(String, String)]): Unit = {
    val sql = "select * from flink.wc where word='" + key + "';"
    val result: CassandraInputFormat[(String, String)] = new CassandraInputFormat[(String, String)](sql, cassandraBuilder)
    val ret: (String, String) = result.nextRecord(Tuple2[String, String])
    out.collect(ret)
  }
}

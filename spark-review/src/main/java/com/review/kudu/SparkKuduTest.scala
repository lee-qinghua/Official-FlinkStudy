package com.review.kudu

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkKuduTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkKuduTest").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //获取 sparkContext 对象
    val sc: SparkContext = sparkSession.sparkContext
    sc.setLogLevel("warn")
    //构建 KuduContext 对象
    val kuduContext = new KuduContext("node1:7051,node2:7051,node3:7051",sc)



  }
}

package com.review.spark_core

import org.apache.spark.{SparkConf, SparkContext}

object D01_mappartitions {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(sparkConf)

    val data = sc.makeRDD(List(1, 2, 3, 4),2)

    val rdd = data.mapPartitions(iter => {
      iter.filter(_ % 2 == 1)
    })

    rdd.foreachPartition(iter=>{
      val rdd_1 = sc.makeRDD(iter.toSeq)

    })

    rdd.collect().foreach(println)
    sc.stop()
  }
}

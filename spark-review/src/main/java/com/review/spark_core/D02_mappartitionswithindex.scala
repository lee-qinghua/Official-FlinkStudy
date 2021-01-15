package com.review.spark_core

import org.apache.spark.{SparkConf, SparkContext}

object D02_mappartitionswithindex {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(sparkConf)

    val data = sc.makeRDD(List(1, 2, 3, 4),2)

    //给数据都加上分区号
    val rdd = data.mapPartitionsWithIndex((index, iter) => {
      iter.map((index, _))
    })



    rdd.collect().foreach(println)
    sc.stop()
  }
}

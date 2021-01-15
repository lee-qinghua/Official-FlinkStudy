package com.review.spark_core

import org.apache.spark.{SparkConf, SparkContext}

object D03_flatmap {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(sparkConf)

    val data = sc.makeRDD(List(List(1, 2), List(3, 4)))

    val rdd = data.flatMap(_.toList)

    rdd.collect().foreach(println)
    sc.stop()
  }
}

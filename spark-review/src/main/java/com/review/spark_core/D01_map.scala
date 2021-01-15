package com.review.spark_core

import org.apache.spark.{SparkConf, SparkContext}

object D01_map {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(sparkConf)

    val data = sc.makeRDD(List(1, 2, 3, 4))

    val mapRDD = data.map(_ * 2)

    mapRDD.collect().foreach(println)
    sc.stop()
  }
}

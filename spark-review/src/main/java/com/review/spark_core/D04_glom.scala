package com.review.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object D04_glom {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(sparkConf)

    val data = sc.makeRDD(List(1, 2, 3, 4), 2)

    val rdd: RDD[Array[Int]] = data.glom()


    rdd.collect().foreach(println)
    sc.stop()
  }
}

package com.review.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object D10_sortby {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(6,2,4,5,3,1), 2)

    rdd.mapPartitions(iter=>{
      iter
    })

    val newRDD: RDD[Int] = rdd.sortBy(num=>num)

    newRDD.saveAsTextFile("output")
    sc.stop()
  }
}

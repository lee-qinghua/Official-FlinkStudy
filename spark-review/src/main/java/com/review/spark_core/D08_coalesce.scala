package com.review.spark_core

import org.apache.spark.{SparkConf, SparkContext}

object D08_coalesce {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(sparkConf)

    val data = sc.makeRDD(List(1, 2, 3, 4), 4)

    // coalesce方法默认情况下不会将分区的数据打乱重新组合
    // 这种情况下的缩减分区可能会导致数据不均衡，出现数据倾斜
    // 如果想要让数据均衡，可以进行shuffle处理
    val value1 = data.coalesce(1)

    //数据打乱重新分区
    val value2 = data.coalesce(2, shuffle = true)

    value2.collect().foreach(println)
    sc.stop()
  }
}

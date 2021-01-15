package com.review.spark_core

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}

object D06_case1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(sparkConf)

    val data = sc.textFile("D:\\peoject\\Official-FlinkStudy\\spark-review\\src\\main\\resources\\data\\apache.log")


    val data1 = data.map(x => {
      val arr = x.split(" ")
      val time = arr(3)

      val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val date: Date = sdf.parse(time)
      val sdf1 = new SimpleDateFormat("HH")
      val hour: String = sdf1.format(date)

      (hour, 1)
    })
      .groupBy(_._1)

    data1.map(x => {
      (x._1, x._2.size)
    }).collect().foreach(println)


    sc.stop()
  }
}

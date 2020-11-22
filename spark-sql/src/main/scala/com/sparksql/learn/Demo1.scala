package com.sparksql.learn

import org.apache.spark.{SparkConf, SparkContext}

object Demo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("appName").setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.parallelize(Array((1,1),(2,2),(3,3),(1,10)))
    data.map(x=>{(x._1,x._2+1)}).collect().foreach(println)
  }
}

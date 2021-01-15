package com.sparksql

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Demo2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("JoinApp").getOrCreate()

    // 将小表数据获得一个map，这边通过collectAsMap()将rdd转成map
    val peopleInfo: collection.Map[String, String] = spark.sparkContext
      .parallelize(Array(("100", "pk"), ("101", "jepson"))).collectAsMap()

    //通过spark.sparkContext.broadcast(peopleInfo)将其广播出去
    val peopleBroadcast: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(peopleInfo)
    //获得大表数据，将其转化成键值对型的rdd数据
    val peopleDetail: RDD[(String, (String, String, String))] = spark.sparkContext
      .parallelize(Array(("100", "ustc", "beijing"), ("103", "xxx", "shanghai"),("101", "xxx", "guangzhou")))
      .map(x => (x._1, x))

    //使用mapPartitions遍历处理批数据
    // mapPartitions做的事情： 遍历大表的每一行数据  和 广播变量的数据对比 有就取出来，没有就拉倒
    val value: RDD[(String, String, String)] = peopleDetail.mapPartitions(x => {
      //通过peopleBroadcast.value获得map类型的小表数据
      val broadcastPeople: collection.Map[String, String] = peopleBroadcast.value
      //遍历小表数据,判断大表key是否在小表的key中存在
      for((key,value) <- x if broadcastPeople.contains(key))
      //for循环的返回值一个可迭代元组，将数据返回
        yield (key, broadcastPeople.get(key).getOrElse(""), value._2)
    })
    value.foreach(println)


    spark.stop()
  }
}

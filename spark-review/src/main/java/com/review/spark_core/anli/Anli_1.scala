package com.review.spark_core.anli

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Anli_1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("hot item")
    val sc = new SparkContext(sparkConf)

    val data = sc.textFile("D:\\peoject\\Official-FlinkStudy\\spark-review\\src\\main\\resources\\data\\user_visit_action.csv")
    // 2. 统计品类的点击数量：（品类ID，点击数量）
    val clickActionRDD = data.filter(
      action => {
        val datas = action.split("_")
        datas(6) != "-1"
      }
    )

    val clickCountRDD: RDD[(String, Int)] = clickActionRDD.map(
      action => {
        val datas = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)



    val orderActionRDD = data.filter(
      action => {
        val datas = action.split("_")
        datas(8) != "null"
      }
    )

    // orderid => 1,2,3
    // 【(1,1)，(2,1)，(3,1)】
    val orderCountRDD = orderActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(8)
        val cids = cid.split(",")
        cids.map(id=>(id, 1))
      }
    ).reduceByKey(_+_)


    // 4. 统计品类的支付数量：（品类ID，支付数量）
    val payActionRDD = data.filter(
      action => {
        val datas = action.split("_")
        datas(10) != "null"
      }
    )

    // orderid => 1,2,3
    // 【(1,1)，(2,1)，(3,1)】
    val payCountRDD = payActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(10)
        val cids = cid.split(",")
        cids.map(id=>(id, 1))
      }
    ).reduceByKey(_+_)


    //clickCountRDD  orderCountRDD   payCountRDD
    //cogroup = connect + group
    val value: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCountRDD.cogroup(orderCountRDD, payCountRDD)

    val value1 = value.map {
      case (id, (clickIter, orderIter, payIter)) => {
        var clickCnt = 0
        val iter1 = clickIter.iterator
        if (iter1.hasNext) {
          clickCnt = iter1.next()
        }
        var orderCnt = 0
        val iter2 = orderIter.iterator
        if (iter2.hasNext) {
          orderCnt = iter2.next()
        }
        var payCnt = 0
        val iter3 = payIter.iterator
        if (iter3.hasNext) {
          payCnt = iter3.next()
        }
        (id, (clickCnt, orderCnt, payCnt))
      }
    }
    val tuples = value1.sortBy(_._2, false).take(10)
    // 6. 将结果采集到控制台打印出来
    tuples.foreach(println)

    sc.stop()
  }
}

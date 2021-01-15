package com.review.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object D05_案例1 {
  def main(args: Array[String]): Unit = {
    //创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    //创建 SparkSession 对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    /**
     * 1. 创建数据
     */
    val data_list1 = List(
      ("c1", "20200301"),
      ("c1", "20200102"),
      ("c2", "20200608"),
      ("c3", "20200815"),
      ("c3", "20200816")
    )
    val df1 = spark.createDataFrame(data_list1).toDF("card_num", "dt")
    df1.show()
    val data_list2 = List(
      ("c1", "100", "20200219"),
      ("c1", "101", "20200619"),
      ("c2", "102", "20200619"),
      ("c3", "103", "20200619"),
      ("c3", "104", "20200619")
    )
    val df2 = spark.createDataFrame(data_list2).toDF("card_num", "merchant_num", "dt")
    df2.show()

    /**
     * 2. 处理
     */
    df1.createTempView("df1_table")
    //获取每个卡片第一次被标记欺诈的日期
    val df11 = spark.sql("select card_num,min(dt) as first from df1_table group by card_num")
    //和消费数据join
    df11.join(df2,df11("card_num")===df2("card_mum"),"inner").where("")

    spark.stop()
  }
}

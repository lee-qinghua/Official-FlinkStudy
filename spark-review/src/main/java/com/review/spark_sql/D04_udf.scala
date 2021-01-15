package com.review.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object D04_udf {
  def main(args: Array[String]): Unit = {
    //创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    //创建 SparkSession 对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val data_list = List(("1", "zhangsan", 10, "China"),
      ("2", "lisi", 16, "usa"),
      ("3", "a", 17, "usa"),
      ("4", "b", 18, "China"),
      ("5", "c", 19, "usa"),
      ("6", "d", 20, "Canada"),
      ("7", "e", 21, "Canada"))
    val df1 = spark.createDataFrame(data_list).toDF("id", "name", "age", "country")
    df1.show()
    spark.udf.register("haha", (x: String) => x + "_")

    import spark.implicits._
    val ds = Seq((1, "foo"), (2, "bar")).toDF("id", "text")
    val toUpperCase = udf((s: String) => s.toUpperCase)
    ds.withColumn("text", toUpperCase('text)).show()


    val isAudlt=udf((age:Int)=>age>18)

    df1.withColumn("test", isAudlt(col("age"))).show()

    //停止
    spark.stop()
  }


}

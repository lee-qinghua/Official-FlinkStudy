package com.sparksql.learn

import com.sparksql.learn.MyModel.Student
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.immutable

class QuickStart {
  def main(args: Array[String]): Unit = {
    // 设置 获取 执行环境
    val sparkConf = new SparkConf().setAppName("demo1")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")

    import sparkSession.implicits._ //隐式转换
    val result = sparkSession.sql("select * from table").as[Student].where("score >60")
    result.cache() //存储一下，下面用到result

    //添加外部条件参数
    val dt = "2020-02-02"
    val inclusive: immutable.Seq[Int] = 2.to(5)
    val result2 = sparkSession.sql(s"select * from table where dt='$dt'").as[Student]
    // todo 统计各个班级的人数
    result2
      .mapPartitions(partition => {
        partition.map(stu => (stu.score, 1))
      })
      .groupByKey(_._1) //这一步的结果为 （source,Iter[(stu1,stu2,stu3)]）
      .mapValues(stu=>stu._2) //对values进行一对一映射
      .reduceGroups(_+_)
      .toDF().coalesce(1)
      .write.mode(SaveMode.Overwrite).insertInto("table2")


  }
}

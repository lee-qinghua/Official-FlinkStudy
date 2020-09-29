package com.otis.work.date20200925解析json

object tt {
  def main(args: Array[String]): Unit = {
    val str = "PRM ROW(PB03 ARRAY<ROW(PB030D01 STRING,PB030Q01 STRING,PB030Q02 STRING,PB030R01 STRING)>)"
    //val str = "PC02D ROW(PC02DS01 STRING,PC02DH ARRAY<ROW(PC02DD01 STRING,PC02DS02 STRING,PC02DS03 STRING,PC02DJ01 STRING,PC02DS04 STRING)>)"
    val id1 = str.split("ARRAY")(0).split("ROW")(0).replace(" ", "")
    //    val str2 =str.split("ARRAY")(0).replace(" ","").split(",")
    val str2 = str.split("ARRAY")(0).replace(" ", "")
    val constr = str2.substring(str2.length - 7)
    var id2 = ""
    if (constr.contains(",")) {
      val arr = str2.split(",")
      val length = arr.length
      id2 = arr(length - 1)
    } else {
      val arr = str2.split("\\(")
      val length = arr.length
      id2 = arr(length - 1)
    }

    println(id2)
    println(id1)
    val data = str.split("ARRAY")(1).split("ROW")(1).replace("(", "").replace(")", "").replace(">", "")
    val arr = data.split(",")
    for (elem <- arr.toList) {
      println(elem.split(" ")(0))
    }
  }
}

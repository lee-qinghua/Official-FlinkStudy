package com.otis.work.date20200925解析json

object tt {
  def main(args: Array[String]): Unit = {
    val str = "PRH.PA01.PA01D ROW(PA01DQ01 STRING,PA01DQ02 STRING,PA01DR01 STRING,PA01DR02 STRING)"
    val id = str.split("ROW")(0).replace(" ", "")
    val data = str.split("ROW")(1)
      .replace("(", "")
      .replace(")", "")
      .split(",")
    for (elem <- data.toList) {
      val tag = elem.split(" ")(0)
      val mtype = elem.split(" ")(1)
      if (mtype.equals("STRING")) {
        println(id + "." + tag)
      }
    }
  }
}

package com.otis.scala.company

import java.util

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//  PA01C ROW(PA01CS01 STRING,PA01CH ARRAY<ROW(PA01CD01 STRING,PA01CI01 STRING)>)
//  只是解析string类型的 不解析array类型
object 自动生成demo1 {


  val map: util.HashMap[String, String] = new util.HashMap[String, String]
  map.put("PA01BQ01", "PRH.PA01.PA01B.PA01BQ01")
  map.put("PA01BD01", "PRH.PA01.PA01B.PA01BD01")
  map.put("PA01BI01", "PRH.PA01.PA01B.PA01BI01")
  map.put("PA01BI02", "PRH.PA01.PA01B.PA01BI02")
  map.put("PA01BD02", "PRH.PA01.PA01B.PA01BD02")
  map.put("PA01AI01", "PRH.PA01.PA01A.PA01AI01")
  map.put("PA01AR01", "PRH.PA01.PA01A.PA01AR01")
  map.put("PA01CS01", "PRH.PA01.PA01C.PA01CS01")
  map.put("PA01DQ01", "PRH.PA01.PA01D.PA01DQ01")
  map.put("PA01DQ02", "PRH.PA01.PA01D.PA01DQ02")
  map.put("PA01DR01", "PRH.PA01.PA01D.PA01DR01")
  map.put("PA01DR02", "PRH.PA01.PA01D.PA01DR02")
  map.put("PA01ES01", "PRH.PA01.PA01E.PA01ES01")
  map.put("PB01AD01", "PIM.PB01.PB01A.PB01AD01")
  map.put("PB01AR01", "PIM.PB01.PB01A.PB01AR01")
  map.put("PB01AD02", "PIM.PB01.PB01A.PB01AD02")
  map.put("PB01AD03", "PIM.PB01.PB01A.PB01AD03")
  map.put("PB01AD04", "PIM.PB01.PB01A.PB01AD04")
  map.put("PB01AQ01", "PIM.PB01.PB01A.PB01AQ01")
  map.put("PB01AQ02", "PIM.PB01.PB01A.PB01AQ02")
  map.put("PB01AD05", "PIM.PB01.PB01A.PB01AD05")
  map.put("PB01AQ03", "PIM.PB01.PB01A.PB01AQ03")
  map.put("PB01BS01", "PIM.PB01.PB01B.PB01BS01")
  map.put("PB020D01", "PMM.PB02.PB020D01")
  map.put("PB020Q01", "PMM.PB02.PB020Q01")
  map.put("PB020D02", "PMM.PB02.PB020D02")
  map.put("PB020I01", "PMM.PB02.PB020I01")
  map.put("PB020Q02", "PMM.PB02.PB020Q02")
  map.put("PB020Q03", "PMM.PB02.PB020Q03")
  map.put("PC010Q01", "PSM.PC01.PC010Q01")
  map.put("PC010Q02", "PSM.PC01.PC010Q02")
  map.put("PC010S01", "PSM.PC01.PC010S01")
  map.put("PC010D01", "PSM.PC01.PC010D01")
  map.put("PC02AS01", "PCO.PC02.PC02A.PC02AS01")
  map.put("PC02AS02", "PCO.PC02.PC02A.PC02AS02")
  map.put("PC02BS01", "PCO.PC02.PC02B.PC02BS01")
  map.put("PC02BJ01", "PCO.PC02.PC02B.PC02BJ01")
  map.put("PC02BS02", "PCO.PC02.PC02B.PC02BS02")
  map.put("PC02CS01", "PCO.PC02.PC02C.PC02CS01")
  map.put("PC02CJ01", "PCO.PC02.PC02C.PC02CJ01")
  map.put("PC02DS01", "PCO.PC02.PC02D.PC02DS01")
  map.put("PC02ES01", "PCO.PC02.PC02E.PC02ES01")
  map.put("PC02ES02", "PCO.PC02.PC02E.PC02ES02")
  map.put("PC02EJ01", "PCO.PC02.PC02E.PC02EJ01")
  map.put("PC02EJ02", "PCO.PC02.PC02E.PC02EJ02")
  map.put("PC02EJ03", "PCO.PC02.PC02E.PC02EJ03")
  map.put("PC02FS01", "PCO.PC02.PC02F.PC02FS01")
  map.put("PC02FS02", "PCO.PC02.PC02F.PC02FS02")
  map.put("PC02FJ01", "PCO.PC02.PC02F.PC02FJ01")
  map.put("PC02FJ02", "PCO.PC02.PC02F.PC02FJ02")
  map.put("PC02FJ03", "PCO.PC02.PC02F.PC02FJ03")
  map.put("PC02GS01", "PCO.PC02.PC02G.PC02GS01")
  map.put("PC02GS02", "PCO.PC02.PC02G.PC02GS02")
  map.put("PC02GJ01", "PCO.PC02.PC02G.PC02GJ01")
  map.put("PC02GJ02", "PCO.PC02.PC02G.PC02GJ02")
  map.put("PC02GJ03", "PCO.PC02.PC02G.PC02GJ03")
  map.put("PC02HS01", "PCO.PC02.PC02H.PC02HS01")
  map.put("PC02HS02", "PCO.PC02.PC02H.PC02HS02")
  map.put("PC02HJ01", "PCO.PC02.PC02H.PC02HJ01")
  map.put("PC02HJ02", "PCO.PC02.PC02H.PC02HJ02")
  map.put("PC02HJ03", "PCO.PC02.PC02H.PC02HJ03")
  map.put("PC02HJ04", "PCO.PC02.PC02H.PC02HJ04")
  map.put("PC02HJ05", "PCO.PC02.PC02H.PC02HJ05")
  map.put("PC02IS01", "PCO.PC02.PC02I.PC02IS01")
  map.put("PC02IS02", "PCO.PC02.PC02I.PC02IS02")
  map.put("PC02IJ01", "PCO.PC02.PC02I.PC02IJ01")
  map.put("PC02IJ02", "PCO.PC02.PC02I.PC02IJ02")
  map.put("PC02IJ03", "PCO.PC02.PC02I.PC02IJ03")
  map.put("PC02IJ04", "PCO.PC02.PC02I.PC02IJ04")
  map.put("PC02IJ05", "PCO.PC02.PC02I.PC02IJ05")
  map.put("PC02KS01", "PCO.PC02.PC02K.PC02KS01")
  map.put("PC030S01", "PNO.PC03.PC030S01")
  map.put("PC040S01", "PPO.PC04.PC040S01")
  map.put("PC05AR01", "PQO.PC05.PC05A.PC05AR01")
  map.put("PC05AD01", "PQO.PC05.PC05A.PC05AD01")
  map.put("PC05AI01", "PQO.PC05.PC05A.PC05AI01")
  map.put("PC05AQ01", "PQO.PC05.PC05A.PC05AQ01")
  map.put("PC05BS01", "PQO.PC05.PC05B.PC05BS01")
  map.put("PC05BS02", "PQO.PC05.PC05B.PC05BS02")
  map.put("PC05BS03", "PQO.PC05.PC05B.PC05BS03")
  map.put("PC05BS04", "PQO.PC05.PC05B.PC05BS04")
  map.put("PC05BS05", "PQO.PC05.PC05B.PC05BS05")
  map.put("PC05BS06", "PQO.PC05.PC05B.PC05BS06")
  map.put("PC05BS07", "PQO.PC05.PC05B.PC05BS07")
  map.put("PC05BS08", "PQO.PC05.PC05B.PC05BS08")

  case class Info(tableName: String, fieldName: String, fieldType: String, tag: String)

  //PRH.PA01.PA01B ROW(PA01BQ01 STRING,PA01BD01 STRING,PA01BI01 STRING,PA01BI02 STRING,PA01BD02 STRING)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[String] = env.socketTextStream("10.1.30.10", 7777)
    stream
      .map(str => {
        //ICR_ONEOFF	管理机构数	oneoff_org_num	int		PC02ES01
        val arr = str.split("\t")
        Info(arr(0), arr(2), arr(3), arr(5))
      })
      .keyBy(_.tableName)
      .timeWindow(Time.seconds(5))
      .process(new MyProFunction2)
      .print()

    env.execute("relation")
  }

  class MyProFunction2 extends ProcessWindowFunction[Info, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Info], out: Collector[String]): Unit = {
      val list = elements.toList
      //生成代码
      val builder = new StringBuilder
      builder.append(" //===========================================================================================================================================").append("\n")
      builder.append("//                                                          todo ").append(key).append("\n")
      builder.append(" //===========================================================================================================================================").append("\n")
      builder.append("val ").append(key).append("=\n")


      builder.append("createView(tableEnv,").append(key).append(",\"").append(key).append("\")").append("\n")
      builder.append("//    tableEnv.sqlQuery(\"select * from ").append(key).append("\").toAppendStream[Row].print()").append("\n")

      // 生成sql语句
      val builder1 = new StringBuilder
      builder1.append("select \n")
      for (i <- list.indices) {
        val fieldType = list(i).fieldType
        val fieldName = list(i).fieldName
        val tableName = list(i).tableName
        val tag = list(i).tag
        if (i == 0) {
          builder1.append("PRH.PA01.PA01A.PA01AI01                 as report_id,\n")
        } else {
          if (fieldName.toUpperCase().equals("SID")) {
            builder1.append("PRH.PA01.PA01A.PA01AI01                 as SID")
          } else if (fieldName.equals("STATISTICS_DT")) {
            builder1.append("'2020-09-27'                            as STATISTICS_DT")
          } else {
            val content = map.get(tag)
            if (fieldType.contains("varchar")) {
              builder1.append(content + "                 as " + fieldName)
            } else if (fieldType.contains("int")) {
              builder1.append("cast(" + content + " as bigint)" + "                 as " + fieldName)
            } else if (fieldType.contains("decimal")) {
              builder1.append("cast(" + content + " as " + fieldType + ")" + "                 as " + fieldName)
            }
          }
          if (i != list.size - 1) {
            builder1.append(",\n")
          } else {
            builder1.append("\n")
          }
        }
      }
      builder1.append("from ods_table")
      builder.append(builder1)
      out.collect(builder.toString())
    }
  }

}
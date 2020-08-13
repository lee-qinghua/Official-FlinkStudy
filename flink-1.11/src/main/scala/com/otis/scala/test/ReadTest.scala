package com.otis.scala.test


import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object ReadTest {

  //17111-1,1,摘要,包含,关键词,退款,17111
  /**
   * @param id            规则编号
   * @param number        序号
   * @param matchField    依据字段：9个字段
   * @param compareMethod 规则运算符：包含，不包含，等于，以*结尾
   * @param keywordType   判别方式：词库编码，关键词，数值
   * @param keyword       命中的内容：1.一个词 2.两个关键词 3.一组数字
   * @param code          返回的code
   */
  case class Data(id: String, number: String, matchField: String, compareMethod: String, keywordType: String, keyword: String, code: String)

  def main(args: Array[String]): Unit = {

  }
}
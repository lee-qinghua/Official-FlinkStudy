package com.otis.scala.test

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object B {

  /**
   * @param id            规则编号
   * @param number        序号
   * @param matchField    依据字段：9个字段
   * @param compareMethod 规则运算符：包含，不包含，等于，以*结尾
   * @param keywordType   判别方式：词库编码，关键词，数值 ，表内其他字段
   * @param keyword       命中的内容：1.一个词 2.两个关键词 3.一组数字
   * @param code          返回的code
   */
  case class Data(id: String, number: String, matchField: String, compareMethod: String, keywordType: String, keyword: String, code: String)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //    val stream: DataStream[String] = env.readTextFile("D:\\project\\finnal_flink\\src\\main\\resources\\a.txt")
    val stream: DataStream[String] = env.socketTextStream("hadoop101", 7777)
    stream
      .map(x => {
        val arr = x.split(",")
        Data(arr(0), arr(1), arr(2), arr(3), arr(4), arr(5), arr(6))
      })
      .keyBy(_.id)
      .timeWindow(Time.seconds(20))
      .process(new MyProFunction)
      .print()

    env.execute("relation")
  }

  class MyProFunction extends ProcessWindowFunction[Data, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Data], out: Collector[String]): Unit = {
      val list = elements.toList
      val builder = new StringBuilder
      builder.append("if ( true ")

      for (elem <- list) {
        builder.append("&& ")
        //获取字段
        val matchField = elem.matchField
        val compareMethod = elem.compareMethod
        val keywordType = elem.keywordType
        val keyword = elem.keyword
        //==========================================================================================================================
        //                                                  todo 摘要
        //==========================================================================================================================
        if ("摘要".equals(matchField)) {
          if ("包含".equals(compareMethod)) {
            if ("关键词".equals(keywordType)) {
              builder.append("isContain(summary,\"").append(keyword).append("\")")
            }
            else if ("词库编码".equals(keywordType)){
              //isInLibiary(summary, new String[]{"14400", "14410", "14430", "14460", "14500", "14510", "14531"}))
              //拼接数组
              val arr: Array[String] = keyword.split("、")
              val arrBuilder = new StringBuilder
              for (i <- arr.indices) {
                if (i == arr.length - 1) {
                  arrBuilder.append("\"").append(arr(i)).append("\"")
                } else {
                  arrBuilder.append("\"").append(arr(i)).append("\", ")
                }
              }
              //组成语句
              builder.append("isInLibiary(summary,new String[]{").append(arrBuilder).append("})")
            }
            else if ("表内其他字段".equals(keywordType)){
              if ("对方户名".equals(keyword)){
                builder.append("isContain(summary,").append(keyword).append(")")
              }
              else if ("账户中文名".equals(keyword)){

              }
            }
          }
          else if ("不包含".equals(compareMethod)) {
            builder.append("isNotContain(summary,").append("\"").append(keyword).append("\")")
          }
          else if ("以结尾".equals(compareMethod)) {
            builder.append("isEndWith(summary,").append("\"").append(keyword).append("\")")
          }
          else if ("以开头".equals(compareMethod)) {
            builder.append("isStartWith(summary,").append("\"").append(keyword).append("\")")
          }
          else if ("等于".equals(compareMethod)) {
            builder.append("isKeywordEqual(summary,").append("\"").append(keyword).append("\")")
          }
        }
        //==========================================================================================================================
        //                                                  todo 对方户名
        //==========================================================================================================================
        else if ("对方户名".equals(matchField)) {
          if ("包含".equals(compareMethod)) {
            builder.append("isContain(accountName,\"").append(keyword).append("\")")
          }
          else if ("不包含".equals(compareMethod)) {
            builder.append("isNotContain(accountName,\"").append(keyword).append("\")")
          }
          else if ("等于".equals(compareMethod)) {
            if ("关键词".equals(keywordType)) {
              builder.append("isKeywordEqual(accountName,\"").append(keyword).append("\")")
            }
            else if ("表内其他字段".equals(keywordType)) {
              //todo 目前只是账户中文名
              builder.append("isKeywordEqual(accountName,accountChinessName)")
            }
          }
          else if ("以开头".equals(compareMethod)) {
            builder.append("isStartWith(accountName,").append("\"").append(keyword).append("\")")
          }
        }
        //==========================================================================================================================
        //                                                  todo 借贷标志
        //==========================================================================================================================
        else if ("借贷标志".equals(matchField)) {
          if ("等于".equals(compareMethod) && "数值".equals(keywordType)) {
            builder.append("isEqual(borrowFlage,").append(keyword).append(")")
          }
        }
        //==========================================================================================================================
        //                                                  todo 账号
        //==========================================================================================================================
        else if ("对方账号".equals(matchField)) {
          if ("以开头".equals(compareMethod) && "词库编码".equals(keywordType)) {
            builder.append("isStartWithLibiary(account,").append("\"").append(keyword).append("\")")
          }
        }
        //==========================================================================================================================
        //                                                  todo 交易金额
        //==========================================================================================================================
        else if ("交易金额".equals(matchField)) {
          if ("小于".equals(compareMethod)) {

            builder.append("Integer.parseInt(transactionAmount)<").append(keyword)
          }
          else if ("大于".equals(compareMethod)) {
            builder.append("Integer.parseInt(transactionAmount)>").append(keyword)
          }
          else if ("等于".equals(compareMethod)) {
            builder.append("Integer.parseInt(transactionAmount)==").append(keyword)
          }
        }
        //==========================================================================================================================
        //                                                  todo 交易代码
        //==========================================================================================================================
        else if ("交易代码".equals(matchField)) {
          if ("等于".equals(compareMethod)) {
            builder.append("isKeywordEqual(transactionCode,").append("\"").append(keyword).append("\")")
          }
        }
        //==========================================================================================================================
        //                                                  todo 业务代号
        //==========================================================================================================================
        else if ("业务代号".equals(matchField)) {
          builder.append("isKeywordEqual(businessCode,").append("\"").append(keyword).append("\")")
        }
        //==========================================================================================================================
        //                                                  todo  账户中文名
        //==========================================================================================================================
        else if ("账户中文名".equals(matchField)) {
          //          builder.append("isKeywordEqual(businessCode,").append("\"").append(keyword).append("\")")
          if ("等于".equals(compareMethod)) {
            builder.append("isKeywordEqual(accountChinessName,").append("\"").append(keyword).append("\")")
          }
          else if ("包含".equals(compareMethod)) {
            builder.append("isContain(accountChinessName,").append("\"").append(keyword).append("\")")
          }
        }
      }
      builder.append(")  return \"").append(elements.head.code).append("\";")

      out.collect(builder.toString())

    }
  }

}


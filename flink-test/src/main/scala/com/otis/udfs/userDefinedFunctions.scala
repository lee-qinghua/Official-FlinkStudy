package com.otis.udfs

import org.apache.flink.table.functions.ScalarFunction

import scala.annotation.varargs


/**
 * @varargs 可以传递多个参数，先进行一次计算，再写一个eval()方法进行一次输出
 */
object Func15 extends ScalarFunction {

  @varargs
  def eval(a: String, b: Int*): String = {
    a + b.length
  }

  def eval(a: String): String = {
    a
  }
}
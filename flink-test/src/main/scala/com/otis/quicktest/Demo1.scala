package com.otis.quicktest

import com.otis.udfs.{Func15, JavaUserDefinedTableFunctions}
import com.otis.utils.StreamTestData
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.types.Row
import org.junit._

import scala.collection.mutable

class Demo1 {

  case class Stu(a: String, b: String, c: Int)

  val settings = EnvironmentSettings.newInstance().useBlinkPlanner().build()
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val tEnv = StreamTableEnvironment.create(env, settings)

  //  @After
  //  def executeAfter: Unit = {
  //    env.execute()
  //  }

  @Test
  def testDemo1: Unit = {
    implicit val sqlquery = "select * from mytable where c < 3"

    val data = List(
      Stu("hello", "java", Int.box(1)),
      Stu("hello", "scala", Int.box(2)),
      Stu("hello", "python", Int.box(3))
    )

    val ds: DataStream[Stu] = env.fromCollection(data)

    // 这里的as 可以加可以不加，用来改变原来的字段名
    val t: Table = tEnv.fromDataStream(ds).as("a", "b", "c")
    tEnv.createTemporaryView("mytable", t)

    tEnv.sqlQuery(sqlquery).toAppendStream[Row].print()
  }

  @Test
  def testDemo2: Unit = {
    val table = StreamTestData.get3TupleDataStream(env).toTable(tEnv).as("a", "b", "c")
    tEnv.createTemporaryView("MyTable", table)

    val sqlquery = "select b,count(a) from MyTable group by b"

    tEnv.sqlQuery(sqlquery).toRetractStream[Row].print()
  }

  @Test
  def testDemo3: Unit = {
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "Hi"))
    data.+=((1, 1L, "Hi World"))
    data.+=((1, 1L, "Test"))
    data.+=((2, 1L, "Hi World"))
    data.+=((2, 1L, "Test"))
    data.+=((3, 1L, "Hi World"))
    data.+=((3, 1L, "Hi World"))
    data.+=((3, 1L, "Hi World"))
    data.+=((4, 1L, "Hi World"))
    data.+=((4, 1L, "Test"))

    val t = env.fromCollection(data).toTable(tEnv).as("a", "b", "c")
    tEnv.createTemporaryView("MyTable", t)

    val sql_1 = s"select a, count(DISTINCT b) as distinct_b, count(DISTINCT c) as distinct_c from MyTable group by a"
    val sql_2 = s"select distinct_b, COUNT(DISTINCT distinct_c) from ($sql_1) group by distinct_b"

    tEnv.sqlQuery(sql_2).toRetractStream[Row].print()
    env.execute()
  }

  @Test
  def testDemo4: Unit = {
    val sqlQuery = "SELECT b, COLLECT(a) FROM MyTable GROUP BY b"

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv).as("a", "b", "c")
    tEnv.createTemporaryView("MyTable", t)

    val result = tEnv.sqlQuery(sqlQuery).toRetractStream[Row].print()

    env.execute()
  }

  @Test
  def testDemo5: Unit = {
    val sqlQuery = "SELECT * FROM T1 " +
      "UNION ALL " +
      "SELECT * FROM T2"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as("a", "b", "c")
    tEnv.createTemporaryView("T1", t1)
    val t2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as("a", "b", "c")
    tEnv.createTemporaryView("T2", t2)

    tEnv.sqlQuery(sqlQuery).toAppendStream[Row].print()
    env.execute()
  }

  @Test
  def testUnnest1: Unit = {
    val data = List(
      (1, Array(12, 45), Array(Array(12, 45))),
      (2, Array(41, 5), Array(Array(18), Array(87))),
      (3, Array(18, 42), Array(Array(1), Array(45)))
    )
    val tab = env.fromCollection(data).toTable(tEnv).as("a", "b", "c")
    tEnv.createTemporaryView("T", tab)
    val sqlQuery1 = "SELECT a, b, s FROM T, UNNEST(T.b) AS A (s)"

    //可以进行过滤
    val sqlQuery2 = "SELECT a, b, s, t FROM T, UNNEST(T.b) AS A (s, t) WHERE s > 13"

    tEnv.sqlQuery(sqlQuery1).toAppendStream[Row].print()
    env.execute()
  }

  @Test
  def testUnnest2: Unit = {

    val data = List(
      (1, 1, (12, 45.6)),
      (2, 2, (12, 45.612)),
      (3, 2, (13, 41.6)),
      (4, 3, (14, 45.2136)),
      (5, 3, (18, 42.6)))
    val tab = env.fromCollection(data).toTable(tEnv).as("a", "b", "c")
    tEnv.createTemporaryView("t1", tab)

    val t2 = tEnv.sqlQuery("SELECT b, COLLECT(c) as aset FROM t1 GROUP BY b")
    tEnv.createTemporaryView("t2", t2)

    tEnv.sqlQuery("select b,id,cast(point as bigint) from t2,unnest(t2.aset) as info(id,point) where b < 3").toRetractStream[Row].print()
    val expected = List(
      "1,12,45.6",
      "2,12,45.612",
      "2,13,41.6")
    env.execute()
  }

  @Test
  def testUnnestNullValue: Unit = {
    val data = List(
      (1, "1", "Hello"),
      (1, "2", "Hello2"),
      (2, "2", "Hello"),
      (3, null.asInstanceOf[String], "Hello"),
      (4, "4", "Hello"),
      (5, "5", "Hello"),
      (5, null.asInstanceOf[String], "Hello"),
      (6, "6", "Hello"),
      (7, "7", "Hello World"),
      (7, "8", "Hello World"))

    val t1 = env.fromCollection(data).toTable(tEnv).as("a", "b", "c")
    tEnv.createTemporaryView("t1", t1)

    val t2 = tEnv.sqlQuery("SELECT a, COLLECT(b) as `set` FROM t1 GROUP BY a")
    tEnv.createTemporaryView("t2", t2)

    val sqlQuery1 = "SELECT a, s FROM t2 LEFT JOIN UNNEST(t2.`set`) AS A(s) ON TRUE WHERE a < 5"

    // unnest只是把结果集展开，left join 用法一样。这样写会丢失a=3的情况
    val sqlQuery2 = "SELECT a, s FROM t2,UNNEST(t2.`set`) AS A(s) WHERE a < 5"
    val sqlQuery3 = "SELECT a, s FROM t2,UNNEST(t2.`set`) AS A(s) WHERE a < 5 and t2.`set` is not null"

    tEnv.sqlQuery(sqlQuery3).toRetractStream[Row].print()

    env.execute()
  }

  @Test
  def testHopStartEndWithHaving: Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val sqlQueryHopStartEndWithHaving =
      """
        |SELECT
        |  c AS k,
        |  COUNT(a) AS v,
        |  HOP_START(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' MINUTE) AS windowStart,
        |  HOP_END(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' MINUTE) AS windowEnd
        |FROM T1
        |GROUP BY HOP(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' MINUTE), c
        |HAVING
        |  SUM(b) > 1 AND
        |    QUARTER(HOP_START(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' MINUTE)) = 1
        |""".stripMargin

    val data = Seq(
      Left(14000005L, (1, 1L, "Hi")),
      Left(14000000L, (2, 1L, "Hello")),
      Left(14000002L, (3, 1L, "Hello")),
      Right(14000010L),
      Left(8640000000L, (4, 1L, "Hello")), // data for the quarter to validate having filter
      Left(8640000001L, (4, 1L, "Hello")),
      Right(8640000010L)
    )
    // env.addSource(new EventTimeSourceFunction[(Int, Long, String)](data)).toTable(tEnv, 'b, 'a, 'c, 'rowtime.rowtime)

  }

  @Test
  def testUDFWithLongVarargs(): Unit = {
    tEnv.registerFunction("func15", Func15)

    val parameters = "c," + (0 until 300).map(_ => "a").mkString(",")
    val sqlQuery = s"SELECT func15($parameters) FROM T1"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as("a", "b", "c")
    tEnv.createTemporaryView("T1", t1)

    tEnv.sqlQuery(sqlQuery).toAppendStream[Row].print()

    env.execute()
  }

  @Test
  def testUDTFWithLongVarargs(): Unit = {
    tEnv.registerFunction("udtf", new JavaUserDefinedTableFunctions.JavaTableFunc1)
    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as("a", "b", "c")
    tEnv.registerTable("T1", t1)

    val parameters = (0 until 300).map(_ => "c").mkString(",")
    val sqlQuery1 =
      s"""
         |select
         |T1.a,
         |T2.x
         |from T1 join lateral table(udtf($parameters)) as T2(x) ON TRUE
         |""".stripMargin
    tEnv.sqlQuery(sqlQuery1).toAppendStream[Row].print()
    env.execute()
  }
  @Test
  def testVeryBigQuery: Unit = {
    val t = StreamTestData.getSingletonDataStream(env).toTable(tEnv).as("a", "b", "c")
    tEnv.registerTable("MyTable", t)

    val sqlQuery = new StringBuilder
    sqlQuery.append("SELECT ")
    val expected = new StringBuilder
    for (i <- 0 until 500) {
      sqlQuery.append(s"a + b + $i, ")
      expected.append((1 + 42L + i).toString + ",")
    }
    sqlQuery.append("c FROM MyTable")
    expected.append("Hi")

    tEnv.sqlQuery(sqlQuery.toString()).toAppendStream[Row].print()
//    println()
//    print(List(expected.toString()))
    env.execute()
  }
}

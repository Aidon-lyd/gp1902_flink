package com.qianfeng.sql

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
 * sql的api
 */
object Demo05_table_sql {
  def main(args: Array[String]): Unit = {
    //获取表执行环境 --- 使用流或者是批次的
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //使用该env创建表的执行环境
    val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env)

   /* //接入数据源 data province adds possibes
    val ds: DataStream[(String, String, Long, Long)] = env.socketTextStream("hadoop01", 6666)
      .map(line => {
        val fields: Array[String] = line.split(" ")
        (fields(0), fields(1), fields(2).trim.toLong, fields(3).trim.toLong)
      })

    //table操作需要基于StreamTableEnvironment，，为表获取数据源
    //流转换成table
    //引入flink的table的api包
    import org.apache.flink.table.api.scala._
    //fileds 不能写单引号和双引号，，
    var table: Table = tenv.fromDataStream(ds, 'd, 'p, 'a)

    //对对进行dml---sql
    tenv.sqlQuery(
      s"""
        |select
        |*
        |from $table
        |where a > 10
        |""".stripMargin)
        .toAppendStream[Row]
        .print("sql-")*/

    //接入数据源 data province adds possibes
    val ds: DataStream[YQ1] = env.socketTextStream("hadoop01", 6666)
      .map(line => {
        val fields: Array[String] = line.split(" ")
        YQ1(fields(0), fields(1), fields(2).trim.toInt, fields(3).trim.toInt,fields(4).trim.toLong)
      })
    import org.apache.flink.table.api.scala._
    //fileds 不能写单引号和双引号，，
    var table: Table = tenv.fromDataStream(ds)

   /* //对对进行dml---sql
    tenv.sqlQuery(
      s"""
         |select
         |dt,
         |province,
         |add,
         |possible
         |from $table
         |where add > 10
         |""".stripMargin)
      .toAppendStream[Row]
      .print("sql-")*/

    tenv.sqlQuery(
      s"""
         |select
         |dt,
         |province,
         |sum(add),
         |sum(possible)
         |from $table
         |group by dt,province
         |""".stripMargin)
      .toRetractStream[Row]
      .print("sql-")

    //触发执行
    env.execute("sql api")
  }
}

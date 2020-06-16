package com.qianfeng.sql

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
 * table的api
 */
object Demo07_table_WC {
  def main(args: Array[String]): Unit = {
    //获取表执行环境 --- 使用流或者是批次的
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //使用该env创建表的执行环境
    val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //接入数据源 data province adds possibes

    val ds: DataStream[(String, Int)] = env.socketTextStream("hadoop01", 6666)
      .flatMap(_.split(" "))
      .map((_, 1))

    //table操作需要基于StreamTableEnvironment，，为表获取数据源
    //流转换成table
    import org.apache.flink.table.api.scala._
    var table: Table = tenv.fromDataStream(ds,'word,'cnt)

    tenv.sqlQuery(
      s"""
         |select
         |word,
         |sum(cnt)
         |from $table
         |group by word
         |""".stripMargin)
        .toRetractStream[Row]
        .print("sql wc-")

    //触发执行
    env.execute("sql wc")
  }
}

package com.qianfeng.sql

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
 * table的api
 */
object Demo02_table_alias {
  def main(args: Array[String]): Unit = {
    //获取表执行环境 --- 使用流或者是批次的
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //使用该env创建表的执行环境
    val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //接入数据源 data province adds possibes

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

    //对对进行dml
    //table = table.select("*")
    //table = table.select("d,p,a")
    table = table
      .groupBy("d,p")
      .select("d,p,count(a)")

    //将表转换成流 --- 必须使用泛型
    //tenv.toAppendStream[Row](table).print()
    tenv.toRetractStream[Row](table).print()

    //触发执行
    env.execute("table api")
  }
}

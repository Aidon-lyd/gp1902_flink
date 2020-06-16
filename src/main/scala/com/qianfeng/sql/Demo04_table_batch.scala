package com.qianfeng.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row

/**
 * table的api
 */
object Demo04_table_batch {
  def main(args: Array[String]): Unit = {
    //获取表执行环境 --- 使用流或者是批次的
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //使用该env创建表的执行环境
    val tenv: BatchTableEnvironment = BatchTableEnvironment.create(env)

    val ds: DataSet[(String,Int)] = env.readCsvFile("E://flinkdata//test.csv")

    //table操作需要基于StreamTableEnvironment，，为表获取数据源
    //流转换成table
    //引入flink的table的api包
    import org.apache.flink.table.api.scala._
    //fileds 不能写单引号和双引号，，
    var table: Table = tenv.fromDataSet(ds,'name,'age)

    //对对进行dml
      table
      .groupBy('name)
        .select('name,'age.sum)
        .toDataSet[Row]
        .print()
  }
}

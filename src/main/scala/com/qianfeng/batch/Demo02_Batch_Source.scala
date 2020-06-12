package com.qianfeng.batch

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/**
 * 批次的source
 * 1、读数据库和文件系统的比较
 * 2、java版本和scala版本类型不一样
 */
object Demo02_Batch_Source {
  def main(args: Array[String]): Unit = {
    val env:ExecutionEnvironment  = ExecutionEnvironment.getExecutionEnvironment

    //直接读取csv中的数据
    env.readCsvFile[(String,String)]("E://flinkdata//test.csv").print()

    //基于集合
    env.generateSequence(1,10000).print()

    //基于createInput
    val fieldTypes: Array[TypeInformation[_]] = Array[TypeInformation[_]](
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO)
    //将基础类型转换成行类型
    val rowTypeInfo = new RowTypeInfo(fieldTypes:_*)
    //获取jdbc的输入格式类
    val jdbcInputFormat: JDBCInputFormat = JDBCInputFormat
      .buildJDBCInputFormat
      .setDrivername("com.mysql.jdbc.Driver")
      .setDBUrl("jdbc:mysql://hadoop01:3306/test")
      .setUsername("root")
      .setPassword("root")
      .setQuery("select * from stu1")
      .setRowTypeInfo(rowTypeInfo)
      .finish
    env.createInput(jdbcInputFormat).print()
  }
}

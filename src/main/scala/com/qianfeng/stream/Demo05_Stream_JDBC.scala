package com.qianfeng.stream
import org.apache.flink.api.common.io.{FileInputFormat, InputFormat}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

/**
 *flink-jdbc的依赖实现
 */
object Demo05_Stream_JDBC {
  def main(args: Array[String]): Unit = {
    //1、流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2、自定义sourcefunction
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

    //使用createInput方式读取数据源
    val jdbcStream: DataStream[Row] = env.createInput(jdbcInputFormat)
    jdbcStream.print("jdbc->")

    //触发执行
    env.execute("jdbc source")
  }
}

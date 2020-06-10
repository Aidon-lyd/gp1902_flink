package com.qianfeng.stream

import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringSchema}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
/**
 * *流式的基础sink
 */
object Demo14_Stream_Sink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    //1、writeAsText

    //2、writecsv --->只能Tuple类型的数据
   /* val csvstream: DataStream[(String, String)] = env.readTextFile("E://flinkdata//test.csv", "utf-8")
      .map(line => {
        val kvs: Array[String] = line.split(",")
        (kvs(0), kvs(1))
      })

    csvstream.writeAsCsv("E://flinkdata//test1.txt")
    csvstream.writeAsCsv("E://flinkdata//test1.txt",WriteMode.OVERWRITE)
    csvstream.writeAsCsv("hdfs://hadoop01:9000/flink_1902/test1.txt",WriteMode.OVERWRITE)
*/
   //3、writeToSocket
    val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    dstream.writeToSocket("hadoop01",6666,new SerializationSchema[String]{
      override def serialize(element: String): Array[Byte] = {
        element.getBytes
      }
    })

    //触发执行
    env.execute("connect")
  }
}
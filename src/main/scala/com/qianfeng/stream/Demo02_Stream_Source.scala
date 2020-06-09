package com.qianfeng.stream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.mutable.ListBuffer

/**
 * flink的流式的source
 */
object Demo02_Stream_Source {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2、获取source
    //需要引入...flink.api.scala包的影视
    val dstream1: DataStream[String] = env.fromElements("hello qianfeng hello flink flink flink is nice")
    dstream1.print("dstream1->")
    println("===============================")
    val li: ListBuffer[Int] = new ListBuffer()
    li += 66
    li += 166
    li += 56
    val dstream2: DataStream[Int] = env.fromCollection(li)
    dstream2.print("dstream2->")

    //基于socket
    val dstream3: DataStream[String] = env.socketTextStream("hadoop01", 6666)

    //基于文件
    val dstream4: DataStream[String] = env.readTextFile("E://flinkdata//test.csv", "utf-8")
    dstream4.print("dstream4->")

    println("=======================")
    val dstream5: DataStream[String] = env.readTextFile("hdfs://hadoop01:9000/words", "utf-8")
    dstream5.print("dstream5->")

    //5、触发执行
    env.execute("stream-wc-scala")
  }
}

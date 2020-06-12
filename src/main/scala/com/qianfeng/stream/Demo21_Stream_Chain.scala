package com.qianfeng.stream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner

/**
 * flink的并行度
 */
object Demo21_Stream_Chain {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)

    //3、进行chain
    dstream.flatMap(_.split(" "))
      .map((_, 1))
      .startNewChain()
      .map((1,_))
      .keyBy(0)
      .sum(1)
      .disableChaining()
      .print("chain->")

    //5、触发执行
    env.execute("parallel")

  }
}

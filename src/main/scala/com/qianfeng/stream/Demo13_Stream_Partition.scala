package com.qianfeng.stream

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

/**
 * partition=>
 * shuffle :
 * DataStream → DataStream
 * 随机进行充分区
 *
 * rebalance：
 * DataStream → DataStream
 * 轮询将数据分布到不同的分区中 --->适用于数据倾斜
 *
 * rescale：
 * DataStream → DataStream
 * 扩展重分区
 *
 * Custom partitioning：
 * 自定义重分区
 */
object Demo13_Stream_Partition {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2、获取source  uid uname tempr timestamp location
    val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
        //.setParallelism(2)

    //shuffle
    dstream.shuffle.print("shuffle->").setParallelism(4)
    //reblanace
    dstream.rebalance.print("rebalance->").setParallelism(4)
    //resscala
    dstream.rescale.print("rescala->").setParallelism(4)


    //触发执行
    env.execute("connect")
  }
}
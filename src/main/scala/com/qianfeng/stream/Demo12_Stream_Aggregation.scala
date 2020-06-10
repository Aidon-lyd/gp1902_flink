package com.qianfeng.stream

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, KeyedStream, SplitStream, StreamExecutionEnvironment}

/**
 * aggreation的算子有：
 * min
 * minBy
 * max
 * maxBy
 * sum
 */
object Demo12_Stream_Aggregation {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2、获取source  uid uname tempr timestamp location
    //val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    val dstream: DataStream[(Int, Int)] = env.fromElements(Tuple2(200, 33), Tuple2(200, 65), Tuple2(200, 101), Tuple2(100, 66), Tuple2(100, 76))

    val keyStream: KeyedStream[(Int, Int), Tuple] = dstream.keyBy(0)

    //聚合
    keyStream.min(1).print("min-")
    keyStream.minBy(1).print("minBy-")
    //keyStream.max(1).print("max-")
    //keyStream.maxBy(1).print("maxBy-")
    //keyStream.sum(1).print("sum-")

    //触发执行
    env.execute("connect")
  }
}
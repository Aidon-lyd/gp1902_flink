package com.qianfeng.stream

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, SplitStream, StreamExecutionEnvironment}

/**
 *
 * 分组和聚合流
 * keyBy:
 * DataStream ---> Keyedstream
 * 是将相同的key分到一组中，类似于sql中group by
 *
 * reduce:
 * KeyedStream--->DataStream
 * 将相同组的数据进行合并计算或者聚合计算，，接收多个值，返回一个值。和keyby搭配使用
 */
object Demo11_Stream_KeyByReduce {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2、获取source  uid uname tempr timestamp location
    //val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    val dstream: DataStream[(Int, Int)] = env.fromElements(Tuple2(200, 33), Tuple2(200, 65), Tuple2(200, 101), Tuple2(100, 66), Tuple2(100, 76))

    dstream.keyBy(0)
        .reduce((kv1,kv2)=>(kv1._1,kv1._2+kv2._2)) //对相同的key进行累加
        .print("reduce->")

    //触发执行
    env.execute("connect")
  }
}

//自定义reducefunction
/*class myReduce extends ReduceFunction[(Int,Int),(Int,Int)]{
  override def reduce(value1: (Int, Int), value2: (Int, Int)): (Int, Int) = ???
}*/

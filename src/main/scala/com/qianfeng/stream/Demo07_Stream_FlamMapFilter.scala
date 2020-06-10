package com.qianfeng.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
/**
 * filter\map\flatmap
 */
object Demo07_Stream_FlamMapFilter {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2、获取source  id timestamp url
    val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    //统计pv，，要求是url的值以http开始，，才统计
    val pvs: DataStream[(String, Int)] = dstream.filter(_.contains("http://"))
      .map(line => {
        val fields: Array[String] = line.split(" ")
        (fields(2), 1)
      })
      .keyBy(0)
      .sum(1)
    //输出
    pvs.print("pvs->")

    //触发执行
    env.execute("flilter map flatmap")
  }
}

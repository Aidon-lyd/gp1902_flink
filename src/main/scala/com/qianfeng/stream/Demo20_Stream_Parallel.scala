package com.qianfeng.stream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * flink的并行度
 */
object Demo20_Stream_Parallel {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2、获取source
    //需要引入...flink.api.scala包的影视
    //val dstream: DataStream[String] = env.fromElements("hello qianfeng hello flink flink flink is nice")
    val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)

    //3、对source进行transformer
    val res: DataStream[(String, Int)] = dstream.flatMap(_.split(" "))
      .map((_, 1))
      .setParallelism(20)
      .keyBy(0)
      //.timeWindow(Time.seconds(5))
      .sum(1).setParallelism(23)

    //4、对DataStream进行sink
    res.print("wc->").setParallelism(16)

    //5、触发执行
    env.execute("parallel")
  }
}

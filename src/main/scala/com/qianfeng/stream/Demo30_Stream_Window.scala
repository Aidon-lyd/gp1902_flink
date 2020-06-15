package com.qianfeng.stream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SessionWindowTimeGapExtractor, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * flink的窗口
 */
object Demo30_Stream_Window {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)

    //3、对source进行transformer
    val res: DataStream[(String, Int)] = dstream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      //.timeWindow(Time.seconds(5)) //滚动窗口，基于时间
      //.timeWindow(Time.seconds(10),Time.seconds(5))  //基于时间滑动窗口
      //.countWindow(3) //基于数据条数的滚动窗口，，需要基于相同key
      //.countWindow(5,2) //基于数据条数的滑动窗口，，5：计算多少条数据  2：2条数据触发康扣执行
      //如下是一些带时间分配器
      //.window(TumblingEventTimeWindows.of(Time.seconds(5)))
      //.window(SlidingEventTimeWindows.of(Time.seconds(5),Time.seconds(2)))
      //.window(EventTimeSessionWindows.withGap(Time.seconds(5)))  //基于事件时间的session窗口
      .sum(1)

    //4、对DataStream进行sink
    res.print("wc->")

    //5、触发执行
    env.execute("stream-wc-scala")
  }
}

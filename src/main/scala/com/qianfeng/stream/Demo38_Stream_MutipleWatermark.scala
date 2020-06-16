package com.qianfeng.stream

import java.text.SimpleDateFormat

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 多并行度的水印
 */
object Demo38_Stream_MutipleWatermark {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //需要设置为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(2)
    env.getConfig.setAutoWatermarkInterval(10)  //水位生成设置时间间隔

    //uid uname timestamp
    val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)

    //3、对source进行transformer
    dstream.filter(_.trim.length>0)
        .map(line=>{
          val fields: Array[String] = line.split(" ")
          (fields(0),fields(1),fields(2).trim.toLong)
        })
        //时间戳和水位分配
        .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, String, Long)] {
          var maxTimeStamp = 0L //迄今为止的最大时间戳
          val lateness = 10000L //延迟时长

          //为咯查看方便，将时间戳格式化
          private val fmt: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

          //获取当前水位
          override def getCurrentWatermark: Watermark = {
              //获取当前水印
            new Watermark(maxTimeStamp - lateness)
          }

          //提取时间戳
          override def extractTimestamp(element: (String, String, Long), previousElementTimestamp: Long): Long = {
            //获取当前数据的时间戳
            val now_timestampe = element._3
            maxTimeStamp = Math.max(now_timestampe,maxTimeStamp)
            val now_wateMark = getCurrentWatermark.getTimestamp
            //获取当前线程的id
            val threadId: Long = Thread.currentThread().getId
            println(s"threadId->${threadId} |Event时间->$now_timestampe | ${fmt.format(now_timestampe)} " +
              s"窗口时间->$maxTimeStamp | ${fmt.format(maxTimeStamp)} " +
              s"当前水位时间->$now_wateMark | ${fmt.format(now_wateMark)}")
            now_timestampe
          }
        })
        .keyBy(0)
        .timeWindow(Time.seconds(3))
        .apply(new RichWindowFunction[(String,String,Long),String,Tuple,TimeWindow] {
          private val fmt: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
          override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, String, Long)], out: Collector[String]): Unit = {
            //将input中的数据进行排序
            val lst: List[(String, String, Long)] = input.iterator.toList.sortBy(_._3)
            val start: String = fmt.format(window.getStart)
            val end: String = fmt.format(window.getEnd)

            val res = s"数据-> ${key.getField(0)} " +
              s"事件开始时间->${fmt.format(lst.head._3)} " +
              s"事件结束时间->${fmt.format(lst.last._3)} " +
              s"窗口开始时间->$start " +
              s"窗口结束时间->$end "
            //输出
            out.collect(res)
          }
        })
        .print()

    //5、触发执行
    env.execute("watermark")
  }
}

//
class MyAssaginder(maxOutofOrderness:Long) extends AssignerWithPeriodicWatermarks[(String, String, Long)]{
  override def getCurrentWatermark: Watermark = ???

  override def extractTimestamp(element: (String, String, Long), previousElementTimestamp: Long): Long = ???
}
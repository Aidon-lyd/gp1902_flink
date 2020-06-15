package com.qianfeng.stream

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 自定义触发器
 */
object Demo34_Stream_MyTrigger {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)

    //3、对source进行transformer
    dstream
        .filter(_.trim.length > 0)
      .map(line=>{
        val fields: Array[String] = line.split(" ")
        //province city add
        (fields(1),fields(2).toInt)
      })
      .keyBy(0)
      .timeWindow(Time.seconds(10)) //滚动窗口，基于时间
      .trigger(new MyTrigger)
      .process(new ProcessWindowFunction[(String,Int),(String,Double),Tuple,TimeWindow] {
        //针对输入的数据进行处理
        override def process(key: Tuple, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Double)]): Unit = {
          var cnt = 0
          var totalAdds = 0
          //对cnt和adds做累计
          for(ele<-elements.iterator){
            cnt += 1
            totalAdds += ele._2
          }
          //输出
          out.collect((key.getField(0),totalAdds/cnt*1.0))
        }
      })
      .print()

    //5、触发执行
    env.execute("window")
  }
}

/**
 * 自定义trigger
 * 1、实现Trigger
 * 2、泛型为输入数据类型和窗口类型
 * 3、当数据条数大于5条就触发或者根据处理时间触发
 * 4、
 *
 * 触发器的几个操作：
 * FIRE： 触发窗口操作
 * CONTINUE ： 在该窗口什么都不做
 * PURGE ： 清空窗口的所有元素或者窗口被关闭
 * FIRE_AND_PURGE ： 触发并清空窗口元素
 *
 */
class MyTrigger extends Trigger[(String,Int),TimeWindow]{
  var cnt = 0  //定义计数器
  //每一个数据处理一次
  override def onElement(element: (String, Int), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //注册一个基于处理时间的定时器
    ctx.registerProcessingTimeTimer(window.maxTimestamp())
    println("当前窗口的最大时间戳："+new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date(window.maxTimestamp())))
    //判断cnt是否触发
    if(cnt >= 5){
      //将会触发窗口执行
      cnt = 0
      TriggerResult.FIRE_AND_PURGE  //触发操作
    } else {
      //将计数器累加
      cnt += 1
      TriggerResult.CONTINUE
    }
  }

  //基于处理时间定时器触发的将会执行该方法
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //基于ProcessingTimeTimer定时器的触发
    TriggerResult.FIRE
  }

  //基于Event时间触发的，将执行该操作
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  //清空窗口或者触发器的操作
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    //清楚该定时器
    ctx.deleteProcessingTimeTimer(window.maxTimestamp())
  }
}

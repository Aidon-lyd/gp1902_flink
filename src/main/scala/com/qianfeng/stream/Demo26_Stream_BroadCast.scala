package com.qianfeng.stream

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * flink的广播变量
 */
object Demo26_Stream_BroadCast {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //获取数据源
    //uid name genderFlag addr
    val dstream: DataStream[(String, String, String, String)] = env.socketTextStream("hadoop01", 6666)
      .map(line => {
        val arr: Array[String] = line.split(" ")
        (arr(0), arr(1), arr(2), arr(3))
      })

    //广播一个sex表 --1：男  2：女 3：人妖
    val desc: MapStateDescriptor[Integer, Character] = new MapStateDescriptor(
      "genderInfo",
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.CHAR_TYPE_INFO)
    //创建广播数据集
    val broadcast: DataStream[(Int, Char)] = env.fromElements((1, '男'), (2, '女'), (3, '妖'))
    val broadCastStream: BroadcastStream[(Int, Char)] = broadcast.broadcast(desc)

    //输入流和广播流进行合并，，并处理
    val res: DataStream[(String, String, String, String)] = dstream.connect(broadCastStream)
      .process(new BroadcastProcessFunction[(String, String, String, String), (Int, Char), (String, String, String, String)] {
        //处理输入的每一个数据
        override def processElement(value: (String, String, String, String),
                                    ctx: BroadcastProcessFunction[(String, String, String, String), (Int, Char), (String, String, String, String)]#ReadOnlyContext,
                                    out: Collector[(String, String, String, String)]): Unit = {
          val genderFlag: Int = value._3.toInt
          var genderLab = ctx.getBroadcastState(desc).get(genderFlag)
          if (genderLab == null) {
            genderLab = 'o'
          }
          //输出
          out.collect((value._1, value._2, genderLab.toString, value._4))
        }

        //处理广播变量中的数据
        override def processBroadcastElement(value: (Int, Char),
                                             ctx: BroadcastProcessFunction[(String, String, String, String), (Int, Char), (String, String, String, String)]#Context,
                                             out: Collector[(String, String, String, String)]): Unit = {
          //相当于将数据更新到状态中
          ctx.getBroadcastState(desc).put(value._1, value._2)
        }
      })
    //添加sink
    res.print("broadcast->")

    //5、触发执行
    env.execute("broadcast")
  }
}
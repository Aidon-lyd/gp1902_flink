package com.qianfeng.stream

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 统计每隔5秒某天某省的新增
 */
object Demo32_Stream_Aggregate {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)

    //3、对source进行transformer
    dstream
      .map(line=>{
        val fields: Array[String] = line.split(" ")
        //province add
        (fields(1),fields(2).toInt)
      })
      .keyBy(0)
      .timeWindow(Time.seconds(5)) //滚动窗口，基于时间
        .aggregate(new AggregateFunction[(String,Int),(String,Int,Int),(String,Int)] {
          //创建累加器
          override def createAccumulator(): (String, Int, Int) = ("",0,0)

          //单个并行度做增加
          override def add(value: (String, Int), accumulator: (String, Int, Int)): (String, Int, Int) = {
            val cnt = accumulator._2 + 1
            val sum = value._2 + accumulator._3
            //返回
            (value._1,cnt,sum)
          }

          //当前并行度和之前的合并
          override def merge(a: (String, Int, Int), b: (String, Int, Int)): (String, Int, Int) = {
            (a._1,a._2+b._2,a._3+b._3)
          }

          //返回结果
          override def getResult(accumulator: (String, Int, Int)): (String, Int) = {
            //(accumulator._1,accumulator._3/accumulator._2)
            (accumulator._1,accumulator._3/accumulator._2)
          }
        })
        .print()


    //5、触发执行
    env.execute("window")
  }
}

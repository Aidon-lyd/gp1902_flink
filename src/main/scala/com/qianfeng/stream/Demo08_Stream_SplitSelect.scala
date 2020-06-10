package com.qianfeng.stream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}

/**
 * split:
 * dataStream--->SplitStream
 * 将一个流拆分成一个或者多个流
 *
 * select:
 * splitStream--->DataStream
 * 选择一个或者多个流
 */
object Demo08_Stream_SplitSelect {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2、获取source  uid uname tempr timestamp location
    val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)

    val splitStream: SplitStream[Temp] = dstream.map(line => {
      val fields: Array[String] = line.split(" ")
      val uid: String = fields(0).trim
      val uname: String = fields(1).trim
      val temp: Double = fields(2).trim.toDouble
      val timastamp: Long = fields(3).trim.toLong
      val localtion: String = fields(4).trim
      Temp(uid, uname, temp, timastamp, localtion)
    })
      .split((temp: Temp) => {
        //温度大于38 或者小于 35
        if (temp.temp > 35 && temp.temp <= 38.0) Seq("正常") else Seq("异常")
      })
    //从split流中获取所有值
    //splitStream.print("split->")

    //通常和select搭配使用
    splitStream.select("正常").print("正常温度旅客->")
    splitStream.select("异常").print("异常温度旅客->")

    //触发执行
    env.execute("flilter map flatmap")
  }
}

case class Temp(uid:String,uname:String,temp:Double,timestamp:Long,location:String)

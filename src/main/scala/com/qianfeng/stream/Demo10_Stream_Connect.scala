package com.qianfeng.stream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, SplitStream, StreamExecutionEnvironment}

/**
 *
 * 合并流
 * Connect:
 * DataStream--->DataStream
 * 只能合并两个流，连接流的类型可以不一致；两个流之间可以共享状态(计算状态)，非常有用
 **
 * union:
 * DataStream--->DataStream
 * 可以合并多个流，多个流的数据类型需要一致
 */
object Demo10_Stream_Connect {
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
    val commonStream: DataStream[(String, String)] = splitStream.select("正常").map(line => {
      (line.uid, s"名字为:${line.uname}的旅客温度正常！！！")
    })

    val exceptionStream: DataStream[(String, String, String)] = splitStream.select("异常").map(line => {
      (line.uid, line.uname, s"旅客温度为：${line.temp} 异常！！！")
    })

    //使用connect合并
    val res: ConnectedStreams[(String, String), (String, String, String)] = commonStream.connect(exceptionStream)
    res.map(
      common=>("用户uid："+common._1,"旅客用户名是："+common._2),
      exception=>"用户的uid："+exception._1+"用户名："+exception._2+"温度："+exception._3
    ).print("connect->")

    //commonStream.union(exceptionStream)  //使用union是不能合并不同类型的stream
    
    //触发执行
    env.execute("connect")
  }
}

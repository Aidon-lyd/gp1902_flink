package com.qianfeng.stream

import java.io.File

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.io.{BufferedSource, Source}
import scala.collection.mutable

/**
 * flink的分布式缓存
 */
object Demo27_Stream_DistributedCache {
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
    dstream.print()

    //注册一个分布式缓存文件
    /*
    1 男
    2 女
    3 妖
     */
    env.registerCachedFile("hdfs://hadoop01:9000/flink_1902/sex","genderInfo")

    //处理数据流
    val res: DataStream[(String, String, String, String)] = dstream.map(new RichMapFunction[(String, String, String, String), (String, String, String, String)] {
      //定义一个存储文件数据流
      var bs: BufferedSource = _

      //定义一个存储流解析出来的数据
      var map: mutable.Map[Int, String] = _

      //读取分布式缓存文件
      override def open(parameters: Configuration): Unit = {
        val genderInfoFile: File = getRuntimeContext.getDistributedCache.getFile("genderInfo")
        //将文件加载成流
        bs = Source.fromFile(genderInfoFile)
        //将bs中的数据存储到map中
        val list: List[String] = bs.getLines().toList
        //初始化map
        map = new mutable.HashMap[Int,String]()
        //循环
        for (line <- list) {
          val fields: Array[String] = line.split(" ")
          map.put(fields(0).trim.toInt, fields(1).trim)
        }
      }

      //每行数据执行一次map
      override def map(value: (String, String, String, String)): (String, String, String, String) = {
        val genderLab: String = map.getOrElse(value._3.trim.toInt, "o")
        //返回
        (value._1, value._2, genderLab, value._4)
      }

      //关闭流
      override def close(): Unit = {
        if (bs != null) {
          bs.close()
        }
      }
    })

    res.print("cache")

    //5、触发执行
    env.execute("cache")
  }
}
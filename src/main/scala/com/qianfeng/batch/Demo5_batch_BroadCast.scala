package com.qianfeng.batch


import java.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import scala.collection.mutable
import scala.collection.JavaConversions._

/**
 * flink的广播变量
 */

object Demo5_batch_BroadCast {
  def main(args: Array[String]): Unit = {
    //1、获执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //获取数据源
    //uid name genderFlag addr
    import org.apache.flink.api.scala._
    val dset: DataSet[(Int, String, Int, String)] = env.fromElements((1, "zs", 1, "hanzhou"), (2, "ls", 2, "hanzhou"), (3, "ww", 3, "hanzhou"), (5, "jg", 5, "hanzhou"))

    //创建广播数据集
    val broadcast: DataSet[Map[Int,Char]] = env.fromElements((1,'男'), (2, '女'), (3, '妖')).map(Map(_))

    //使用map来和广播变量中的数据进行操作
    val res: DataSet[(Int, String, Char, String)] = dset.map(new RichMapFunction[(Int, String, Int, String), (Int, String, Char, String)] {

      //定义一个list去接收广播变量
      var bc: util.List[mutable.Map[Int, Char]] = _
      var bcMap: mutable.Map[Int, Char] = _

      //将广播变量中的数据取出存储到map集合中
      override def open(parameters: Configuration): Unit = {
        //java和scala互转

        //初始化bcMap
        bcMap = new mutable.HashMap()
        //取出广播变量中的值
        bc = getRuntimeContext.getBroadcastVariable("genderInfo")
        //将bc中的数据循环存储到bcMap中
        for (perBc <- bc) {
          bcMap.putAll(perBc)
        }
      }

      //每条数据处理一次
      override def map(value: (Int, String, Int, String)): (Int, String, Char, String) = {
        //将每一行数据和bcMap中比较
        val genderLab: Char = bcMap.getOrElse(value._3, 'o')
        //输出
        (value._1, value._2, genderLab, value._4)
      }

      override def close(): Unit = super.close()
    }).withBroadcastSet(broadcast, "genderInfo")

    res.print()
  }
}
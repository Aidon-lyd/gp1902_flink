package com.qianfeng.stream

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * 累计输入数据行数
 * date provnce add possible
 *
 */
object Demo29_Stream_ACC2 {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.socketTextStream("hadoop01", 6666)
      .map(line => {
        val fields: Array[String] = line.split(" ")
        (fields(0) + "_" + fields(1), fields(2).trim.toInt, fields(3).trim.toInt)
      })
      .keyBy(0)
      .map(new RichMapFunction[(String,Int,Int), String] {
        var addCounter: IntCounter = new IntCounter(0)
        var possibleCounter: IntCounter = new IntCounter(0)

        override def open(parameters: Configuration): Unit = {
          //2、注册累加器
          getRuntimeContext.addAccumulator("addCounter", addCounter)
          getRuntimeContext.addAccumulator("possibleCounter", possibleCounter)
        }

        override def map(value: (String, Int, Int)): String = {
          addCounter.add(value._2)
          possibleCounter.add(value._3)
          value +""
        }
      })


    //获取累加器的值 --- 只能任务执行结束才可以看到
    //5、触发执行
    val accres = env.execute("counter")
    val adds: String = accres.getAccumulatorResult("addCounter").toString
    val possibles: String = accres.getAccumulatorResult("possibleCounter").toString
    println(s"adds:${adds},possibles:${possibles}")
  }
}
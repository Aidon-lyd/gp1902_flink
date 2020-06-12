package com.qianfeng.stream

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * 累计输入数据行数
 */
object Demo28_Stream_ACC {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val res: DataStream[String] = env.socketTextStream("hadoop01", 6666)
      .map(new RichMapFunction[String, String] {

        var intCounter: IntCounter = _

        override def open(parameters: Configuration): Unit = {
          //1、创建一个累加
          intCounter = new IntCounter(0)

          //2、注册累加器
          getRuntimeContext.addAccumulator("recordCounter", intCounter)
          //
        }

        //累计输入记录
        override def map(value: String): String = {
          if (value != null) {
            intCounter.add(1)
          }
          //返回
          intCounter + "_" + value
        }
      })

    res.print("===")
    //获取累加器的值 --- 只能任务执行结束才可以看到
    //5、触发执行
    val accres = env.execute("cache")
    val accvalue: String = accres.getAccumulatorResult("recordCounter").toString
    println("累加器最终结果："+accvalue)
  }
}
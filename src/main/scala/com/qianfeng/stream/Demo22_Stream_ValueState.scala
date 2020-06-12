package com.qianfeng.stream

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * flink的并行度
 */
object Demo22_Stream_ValueState {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //获取数据源
    val dstream: DataStream[(Int, Int)] = env.fromElements((11, 5), (11, 2), (11, 3), (11, 6), (22, 3), (22, 1), (22, 2))
    dstream.keyBy(0)
        .flatMap(new MyFlatMapFunction)
        .print()

    //5、触发执行
    env.execute("value state")
  }
}
//自定义flatmap实现分组累加功能
class MyFlatMapFunction extends RichFlatMapFunction[(Int,Int),(Int,Int)]{
  //定义一个valueState
  var sum:ValueState[(Int,Int)] = _
  var count:Int = 0
  //初始化
  override def open(parameters: Configuration): Unit = {
    //定义一个valueState描述器
    val desc: ValueStateDescriptor[(Int, Int)] = new ValueStateDescriptor[(Int, Int)](
      "sum",
      TypeInformation.of(new TypeHint[(Int, Int)] {}),
      (0, 0))
    //获取状态赋值给sum
    sum = getRuntimeContext.getState(desc)
  }

  override def flatMap(value: (Int, Int), out: Collector[(Int, Int)]): Unit = {
    /*//1、分组累加
    val total:Int = sum.value()._2 + value._2
    //更新状态
    sum.update((sum.value()._1,total))

    //输出
    out.collect(value._1,total)*/

    //分组每2条求一次平均值，，2条后需要清0重新计算
     count = sum.value()._1 + 1
    val total:Int = sum.value()._2 + value._2
    //更新状态
    sum.update((count,total))

    if(sum.value()._1 >= 2){
      out.collect(value._1,total/sum.value()._1)
      //清空state
      sum.clear()
    }
  }

  override def close(): Unit = super.close()
}

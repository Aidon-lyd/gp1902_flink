package com.qianfeng.stream

import java.util.Random

import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * flink的流式的自定义source
 */
object Demo03_Stream_Custom {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2、自定义sourcefunction
    val dstream1: DataStream[String] = env.addSource(new SourceFunction[String] {
      //用于产生数据到下游
      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        val random: Random = new Random()
        //死循环产生
        while (true) {
          val round: Int = random.nextInt(100)
          //输出
          ctx.collect("round value :" + round)
          //睡眠
          Thread.sleep(1000)
        }
      }

      //取消，控制线程怎么结束
      override def cancel(): Unit = ???
    })

    dstream1.print("sourceFunction->")

    //2、自定义Richsourcefunction
    val dstream2: DataStream[String] = env.addSource(new MyRichSourceFunction)
    dstream1.print("richSourceFunction->")

    //3、自定义RichParallelsourcefunction
    val dstream3: DataStream[String] = env.addSource(new MyRichParallelFunction).setParallelism(2)
    dstream3.print("RichParallelFunction->")

    //5、触发执行
    env.execute("stream-wc-scala")
  }
}

//自定义的richsourcefunction
class MyRichSourceFunction extends RichSourceFunction[String] {

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def close(): Unit = super.close()

  //用于产生数据到下游
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    val random: Random = new Random()
    //死循环产生
    while (true) {
      val round: Int = random.nextInt(50)
      //输出
      ctx.collect("round value :" + round)
      //睡眠
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = ???
}


//可并行度的富函数
class MyRichParallelFunction extends RichParallelSourceFunction[String]{

  //用于产生数据到下游
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    val random: Random = new Random()
    //死循环产生
    while (true) {
      val round: Int = random.nextInt(10)
      //输出
      ctx.collect("round value :" + round)
      //睡眠
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = ???

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def close(): Unit = super.close()
}

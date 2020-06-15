package com.qianfeng.stream

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.runtime.operators.window.CountWindow
import org.apache.flink.util.Collector

/**
 * 统计最近5秒某省平均的新增数
 */
object Demo33_Stream_Proccess {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)

    //3、对source进行transformer
    dstream
      .map(line=>{
        val fields: Array[String] = line.split(" ")
        //province city add
        (fields(1),fields(2).toInt)
      })
      .keyBy(0)
      .timeWindow(Time.seconds(5)) //滚动窗口，基于时间
        .process(new ProcessWindowFunction[(String,Int),(String,Double),Tuple,TimeWindow] {
          //针对输入的数据进行处理
          override def process(key: Tuple, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Double)]): Unit = {
            var cnt = 0
            var totalAdds = 0
            //对cnt和adds做累计
            for(ele<-elements.iterator){
              cnt += 1
              totalAdds += ele._2
            }
            //输出
            out.collect((key.getField(0),totalAdds/cnt*1.0))
          }
        })
        .print()

    //5、触发执行
    env.execute("window")
  }
}

package com.qianfeng.stream

import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * flink的状态后端设置
 */
object Demo24_Stream_Backkend {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置checkpoint
    val conf = env.getCheckpointConfig   //获取checkpoint的配置
    //checkpoint的其它配置
    //checkpoint的语义
    conf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //checkpoint配置
    conf.isCheckpointingEnabled
    //设置取消保留状态
    conf.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    conf.setCheckpointInterval(1000)  //检测间隔
    //checkpoint的超时设置
    conf.setCheckpointTimeout(60000)
    //最大并行checkpoint
    conf.setMaxConcurrentCheckpoints(6)
    //设置状态后端存储
    /**
     * 三种方式：
     * jm的内存
     * fs
     * rocksDB
     */
    //env.setStateBackend(new MemoryStateBackend())  //状态后端内存
    env.setStateBackend(new FsStateBackend("hdfs://hadoop01:9000/flink_1902/checkpoint"))
    //env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop01:9000/flink_1902/rocksDB_checkpoint",true))
    //StateBackend

    val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)

    //3、对source进行transformer
    val res: DataStream[(String, Int)] = dstream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    //4、对DataStream进行sink
    res.print("wc->")

    //5、触发执行
    env.execute("stream-wc-scala")
  }
}

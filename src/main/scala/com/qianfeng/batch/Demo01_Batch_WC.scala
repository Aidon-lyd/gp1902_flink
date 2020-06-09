package com.qianfeng.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.client.cli.CliFrontend
import org.apache.flink.runtime.executiongraph.ExecutionGraph
import org.apache.flink.runtime.jobgraph.JobGraph
import org.apache.flink.streaming.api.graph.StreamGraph

/**
 * 批次的词频统计应用
 */
object Demo01_Batch_WC {
  def main(args: Array[String]): Unit = {
    //1、获取批次执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    /*StreamGraph
    JobGraph
    ExecutionGraph*/
    //CliFrontend
    //2、获取source
    //val dset: DataSet[String] = env.fromElements("hello qianfneg hello qianfeng")
    val dset: DataSet[String] = env.readTextFile(args(0))

    //3、转换
    val res: AggregateDataSet[(String, Int)] = dset.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    //4、添加sink
    //res.print()
    res.writeAsText(args(1)).setParallelism(1)

    env.execute("")
  }
}

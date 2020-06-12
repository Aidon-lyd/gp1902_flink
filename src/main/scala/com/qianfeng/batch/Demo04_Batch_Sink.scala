package com.qianfeng.batch

import com.qianfeng.stream.{MyMysqlOutputFormat, YQ}
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode


/**
 * 批次的sink
 */
object Demo04_Batch_Sink {
  def main(args: Array[String]): Unit = {
    val env:ExecutionEnvironment  = ExecutionEnvironment.getExecutionEnvironment
    //设置
    env.setParallelism(1)

    val dataset: DataSet[(Int, String)] = env.fromElements((1, "zs"), (2, "mazi"))
    //1、基于文件的输出
    dataset.writeAsCsv("E://flinkdata//test3.csv","\n",",",WriteMode.OVERWRITE)
    dataset.print()
    //dataset.writeAsCsv("hdfs://hadoop01:9000/test2.csv","\n",",",WriteMode.OVERWRITE)

    //2、基于outputformat的
    val dset: DataSet[YQ] = env.fromElements(YQ("2020-06-11", "zhejiang", 22, 33), YQ("2020-06-11", "anhui", 2, 13))

    dset.output(new MyMysqlOutputFormat)
    dset.print()

    //需要出发执行？？？

  }
}

package com.qianfeng.batch

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{ExecutionEnvironment, _}


/**
 * 批次的转换
 */
object Demo03_Batch_Transformation {
  def main(args: Array[String]): Unit = {
    val env:ExecutionEnvironment  = ExecutionEnvironment.getExecutionEnvironment

    val dataset: DataSet[String] = env.fromElements("i link flink flink is nice","flink","flink")

    //常见算子
    //映射
    dataset.map((_,1)).print()
    //flatmap 扁平化
    dataset.flatMap(_.split(" ")).print()
    //过滤
    dataset.filter(_.length > 10).print()
    //去重
    dataset.distinct().print()
    println("===========================")
    //mappartition
    dataset.mapPartition(in => in map { (_, 1) }).print()

    val dset2: DataSet[Int] = env.fromElements(11, 22, 33)
    //使用reduce累加
    dset2.reduce(_+_).print()
    dset2.reduce((x,y)=>x+y).print()

    //聚合
    val dset3: DataSet[(Int, String, Int)] = env.fromElements((1, "zs", 66), (1, "zs", 20), (1, "zs", 10), (2, "ls", 66), (2, "ls", 96))
    dset3.groupBy(0).sum(2).print()

    dset3.aggregate(Aggregations.SUM,2).aggregate(Aggregations.MIN,2).print()

    dset3.sum(2).min(2).print()

    dset3.minBy(2).print()
    dset3.maxBy(2).print()

    println("=================")
    //分组
    dset3.groupBy(0,1).sum(2).print()
    dset3.groupBy(0).sortGroup(2,Order.DESCENDING).first(1).print()

    //join 连接
    val dset4: DataSet[(Int, Int)] = env.fromElements((1, 77), (2, 90), (3, 90))
    val dset5: DataSet[(Int, Int)] = env.fromElements((1, 1), (2, 2), (5, 2))
    println("==========")
    dset4.join(dset5).where(0).equalTo(0).print()
    println("==========")
    //union -- 类型一致
    dset4.union(dset5).print()
    println("==========")
    dset4.cross(dset5).print()
    println("==========")

    //重分区
    dataset.rebalance().print()
    dset5.partitionByHash(0).print()
    //dset5.partitionCustom(new Partitioner[] {})
    dset5.groupBy(1).first(1).print()
  }
}

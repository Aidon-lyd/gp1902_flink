package com.qianfeng.sql

import com.qianfeng.stream.YQ
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
 * table的窗口
 */
object Demo03_table_window {
  def main(args: Array[String]): Unit = {
    //获取表执行环境 --- 使用流或者是批次的
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //使用该env创建表的执行环境
    val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env)


    //接入数据源 data province adds possibes

    val ds: DataStream[YQ1] = env.socketTextStream("hadoop01", 6666)
      .map(line => {
        val fields: Array[String] = line.split(" ")
        YQ1(fields(0), fields(1), fields(2).trim.toInt, fields(3).trim.toInt, fields(4).trim.toLong)
      })
      //maxOutOfOrderness ： 最大乱序时间
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[YQ1](Time.seconds(2)) {
        override def extractTimestamp(element: YQ1): Long = element.timestamp * 1000L
      })

    //table操作需要基于StreamTableEnvironment，，为表获取数据源
    //流转换成table
    //引入flink的table的api包
    import org.apache.flink.table.api.scala._
    //fileds 不能写单引号和双引号，，
    val table: Table = tenv.fromDataStream(ds, 'dt, 'add,'timestamp.rowtime)
      //翻转窗口
      .window(Tumble over 2.second on 'timestamp as 'tt)
      .groupBy('dt, 'tt)
      .select('dt, 'add.sum)


    //将表转换成流 --- 必须使用泛型
    //tenv.toAppendStream[Row](table).print()
    tenv.toRetractStream[Row](table).print()

    //触发执行
    env.execute("table api")
  }
}

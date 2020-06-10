package com.qianfeng.stream

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}
import java.util.Properties

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic

/**
 * 使用flink-kafka连接器的sink
 * 输入疫情数据：
 * date province add possible
 * 2020-06-10 zhejiang 120 230
 * 2020-06-10 zhejiang 12 23
 *
 * 输出:实时输出到mysql中---采用kafka的连接器（根据date和province）
 * 2020-06-10 zhejiang 132 253
 */
object Demo17_Stream_KafkaSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    //实时根据date和province统计数据
    val res: DataStream[String] = dstream.map(line => {
      val fields: Array[String] = line.split(" ")
      (fields(0) + "_" + fields(1), (fields(2).trim.toInt, fields(3).trim.toInt))
    })
      .keyBy(0)
      .reduce((kv1, kv2) => {
        (kv1._1, (kv1._2._1 + kv2._2._1, kv1._2._2 + kv2._2._2))
      })
      .map(line => {
        val date_pro: Array[String] = line._1.split("_")
        date_pro(0) + "_" + date_pro(1) + "_" + line._2._1 + "_" + line._2._2
      })

    //自定义kafka的sink
    val broker_list:String = "hadoop01:9092,hadoop02:9092,hadoop03:9092"
    val toTopic:String = "test"
    val kafkaProducer: FlinkKafkaProducer[String] = new FlinkKafkaProducer[String](broker_list, toTopic, new SimpleStringSchema())
    res.addSink(kafkaProducer)


    val res1: DataStream[YQ] = dstream.map(line => {
      val fields: Array[String] = line.split(" ")
      (fields(0) + "_" + fields(1), (fields(2).trim.toInt, fields(3).trim.toInt))
    })
      .keyBy(0)
      .reduce((kv1, kv2) => {
        (kv1._1, (kv1._2._1 + kv2._2._1, kv1._2._2 + kv2._2._2))
      })
      .map(line => {
        val date_pro: Array[String] = line._1.split("_")
        YQ(date_pro(0), date_pro(1), line._2._1, line._2._2)
      })
   val pro: Properties = new Properties()
    val ka: FlinkKafkaProducer[YQ] = new FlinkKafkaProducer(toTopic, new YQSchema(toTopic), pro, Semantic.EXACTLY_ONCE)
    //将kafka的producer添加即可


    //触发执行
    env.execute("connect")
  }
}
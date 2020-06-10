package com.qianfeng.stream

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 *flink消费kafka中的数据
 */
object Demo06_Stream_KafkaConnector {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2、使用kafka的connector
    val topic:String = "test"
    val pro: Properties = new Properties()
    //使用类加载器加载properties文件
    pro.load(Demo06_Stream_KafkaConnector.getClass.getClassLoader.getResourceAsStream("consumer.properties"))
    //kafka1.0+的整合依赖
    val res: DataStream[String] = env.addSource(new FlinkKafkaConsumer(topic, new SimpleStringSchema(), pro))
    //kafka-0.9的整合依赖
    //val res: DataStream[String] = env.addSource(new FlinkKafkaConsumer09(topic, new SimpleStringSchema(), pro))
    res.print("kafkaconnector->")

    env.execute("kafka connector")
  }
}

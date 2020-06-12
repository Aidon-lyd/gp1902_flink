package com.qianfeng.stream

import java.lang
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}
import java.util.Properties

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.kafka.clients.producer.ProducerRecord

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
    //val kafkaProducer: FlinkKafkaProducer[String] = new FlinkKafkaProducer[String](broker_list, toTopic, new SimpleStringSchema())
    //res.addSink(kafkaProducer)

    //使用FlinkKafkaProducer的新版
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
    res1.print("kafka->")
    val pro: Properties = new Properties()
    pro.setProperty("bootstrap.servers",broker_list)
    val ka: FlinkKafkaProducer[YQ] = new FlinkKafkaProducer(
      toTopic,
      new YQSchema(toTopic),
      pro,
      Semantic.EXACTLY_ONCE)
    //将kafka的producer添加即可
    res1.addSink(ka)

    //触发执行
    env.execute("kafka sink")
  }
}

//自定义kafka的schema
class YQSchema(topic:String) extends KafkaSerializationSchema[YQ]{
  override def serialize(element: YQ, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val dt = element.dt
    val province = element.province
    val adds = element.adds
    val possibles = element.possibles
    new ProducerRecord[Array[Byte], Array[Byte]](topic,(dt+" "+province).getBytes(),(dt+" "+province+" "+adds+" "+possibles).getBytes())
  }
}
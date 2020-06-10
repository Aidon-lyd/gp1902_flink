package com.qianfeng.stream

import java.lang

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

class YQSchema(topic:String) extends KafkaSerializationSchema[YQ]{
  override def serialize(element: YQ, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    new ProducerRecord[Array[Byte], Array[Byte]](topic,Array(element.dt.getBytes()),Array(element.adds.toString.getBytes()))
  }
}

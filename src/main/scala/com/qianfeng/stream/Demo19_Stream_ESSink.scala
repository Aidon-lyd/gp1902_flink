package com.qianfeng.stream

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import java.util

import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests



/**
 * 使用自定义Essink
 * 输入疫情数据：
 * date province add possible
 * 2020-06-10 zhejiang 120 230
 * 2020-06-10 zhejiang 12 23
 *
 * 输出:实时输出到redis根据date和province）
 * 2020-06-10 zhejiang 132 253
 *
 * 三个核心类：
 *ElasticsearchSink.BUilder
 * ElasticsearchSinkFunction
 *
 */
object Demo19_Stream_ESSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    //实时根据date和province统计数据
    val res: DataStream[YQ] = dstream.map(line => {
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

    //自定义ES的sink
    val hosts: util.ArrayList[HttpHost] = new util.ArrayList[HttpHost]()
    hosts.add(new HttpHost("hadoop01",9200,"http"))
    hosts.add(new HttpHost("hadoop02",9200,"http"))
    hosts.add(new HttpHost("hadoop03",9200,"http"))

    //获取一个essink
    val builder: ElasticsearchSink.Builder[YQ] = new ElasticsearchSink.Builder[YQ](hosts, new MyESSink)
    //设置一个桶最大刷新大小
    builder.setBulkFlushMaxSizeMb(10)
    builder.setBulkFlushInterval(1)
    //获取essink实例
    val essink: ElasticsearchSink[YQ] = builder.build()
    //将es的sink添加到addsink
    res.addSink(essink)

    //触发执行
    env.execute("essink")
  }
}

/*
自定义sink
1、需要实现ElasticsearchSinkFunction
 */
class MyESSink extends ElasticsearchSinkFunction[YQ]{
  //每条数据触发执行一次
  override def process(element: YQ, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
      val indexName = "flink_yq"

    //获取map
    val javaMap: util.HashMap[String, String] = new util.HashMap[String, String]()
    javaMap.put("dt",element.dt)
    javaMap.put("province",element.province)
    javaMap.put("adds",element.adds+"")
    javaMap.put("possibles",element.possibles+"")

    //将数据插入到es中
    val request: IndexRequest = Requests.indexRequest()
      .index(indexName)
      .`type`(indexName)
      .id(element.dt + element.province)
      .source(javaMap)
    //将indexrequest放到RequestIndexer
    indexer.add(request)
  }
}
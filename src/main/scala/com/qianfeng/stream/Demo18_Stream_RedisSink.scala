package com.qianfeng.stream

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * 使用自定义redissink
 * 输入疫情数据：
 * date province add possible
 * 2020-06-10 zhejiang 120 230
 * 2020-06-10 zhejiang 12 23
 *
 * 输出:实时输出到redis根据date和province）
 * 2020-06-10 zhejiang 132 253
 *
 * 三个核心类：
 * FlinkJedisPoolConfig.Builder
 * RedisSink
 * RedisMapper
 */
object Demo18_Stream_RedisSink {
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

    //自定义redis的sink
    val builder: FlinkJedisPoolConfig.Builder = new FlinkJedisPoolConfig.Builder()
    val config: FlinkJedisPoolConfig = builder.setHost("hadoop01")
      .setPort(6379)
      .setTimeout(10000)
      .setPassword("root")
      .build()
    //获取一个RedisSink实例
    val redissink: RedisSink[YQ] = new RedisSink(config, new MyRedisSink)
    res.addSink(redissink)

    //触发执行
    env.execute("connect")
  }
}

/*
自定义sink
1、flink-redis的主要redisMapper 和 flinkredisPoolCoffig
2、自定义redis的sink需要
 */
class MyRedisSink extends RedisMapper[YQ]{
  //命令描述 --- 使用redis的什么命令;;;additionKey : 当存储命令为HSET，sort set
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET,null)
  }

  //获取key
  override def getKeyFromData(data: YQ): String = {
    return data.dt+"_"+data.province
    /*
    key dt
    field provice_adds
    value add值

     */
  }

  //获取value值
  override def getValueFromData(data: YQ): String = {
    return data.adds +"_"+data.possibles
  }
}
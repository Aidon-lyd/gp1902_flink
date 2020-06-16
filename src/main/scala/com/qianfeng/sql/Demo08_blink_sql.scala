package com.qianfeng.sql

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * blink sql
 * 消费kafka中的数据
 * 使用sql将业务处理完成之后把结果数据打入到mysql中
 */
object Demo08_blink_sql {
  def main(args: Array[String]): Unit = {
    //获取blink的执行环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tenv: TableEnvironment = TableEnvironment.create(settings)

    //消费kafka中的数据
    tenv.sqlUpdate(
      s"""
         |create table user_log(
         |user_id varchar,
         |item_id varchar,
         |category_id varchar,
         |action varchar,
         |ts TIMESTAMP
         |) with(
         |'connector.type' = 'kafka', -- 使用 kafka connector
         |'connector.version' = 'universal',  -- kafka 版本，universal 支持 0.11 以上的版本
         |'connector.topic' = 'test',  -- kafka topic
         |'connector.properties.0.key' = 'zookeeper.connect',  -- 连接信息
         |'connector.properties.0.value' = 'hadoop01:2181,hadoop02:2181,hadoop03:2181',
         |'connector.properties.1.key' = 'bootstrap.servers',
         |'connector.properties.1.value' = 'hadoop01:9092,hadoop02:9092,hadoop03:9092',
         |'update-mode' = 'append',
         |'connector.startup-mode' = 'latest-offset', -- earliest-offset:从起始 offset 开始读取
         |'format.type' = 'json',  -- 数据源格式为 json
         |'format.derive-schema' = 'true' -- 从 DDL schema 确定 json 解析规则
         |)
         |""".stripMargin)

    //将kafka中的数据流进行统计
    tenv.sqlUpdate(
      s"""
         |INSERT into pvuv_sink_1902
         |SELECT
         |DATE_FORMAT(ts, 'yyyy-MM-dd HH:00') dt,
         |COUNT(*) AS pv,
         |COUNT(DISTINCT user_id) AS uv
         |FROM user_log
         |GROUP BY DATE_FORMAT(ts, 'yyyy-MM-dd HH:00')
         |""".stripMargin)
    //将结果打入msyql
    tenv.sqlUpdate(s"""
                      |CREATE TABLE pvuv_sink_1902 (
                      |dt VARCHAR,
                      |pv BIGINT,
                      |uv BIGINT
                      |) WITH (
                      |'connector.type' = 'jdbc', -- 使用 jdbc connector
                      |'connector.url' = 'jdbc:mysql://hadoop01:3306/test', -- jdbc url
                      |'connector.table' = 'pvuv_sink_1902', -- 表名
                      |'connector.username' = 'root', -- 用户名
                      |'connector.password' = 'root', -- 密码
                      |'connector.write.flush.max-rows' = '1' -- 默认5000条，为了演示改为1条
                      |)
                      |""".stripMargin)

    //触发执行
    tenv.execute("blink sql")
  }
}

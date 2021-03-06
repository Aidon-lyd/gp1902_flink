package com.qianfeng.stream

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
 * 使用自定义mysqlsink
 * 输入疫情数据：
 * date province add possible
 * 2020-06-10 zhejiang 120 230
 * 2020-06-10 zhejiang 12 23
 *
 * 输出:实时输出到mysql中---采用自定义output（根据date和province）
 * 2020-06-10 zhejiang 132 253
 */
object Demo16_Stream_MysqlSink {
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

    //自定义mysql的sink
    res.addSink(new MyMysqlSink)

    //触发执行
    env.execute("connect")
  }
}


/*
自定义sink
1、需要继承SinkFunction或者RichSinnkFunction
2、RichSinkFunction的泛型是输入流中的类型
 */
class MyMysqlSink extends RichSinkFunction[YQ]{
  //mysql的连接对象初始化
  var conn:Connection = _
  var ps:PreparedStatement = _
  var rs: ResultSet = _

  override def open(parameters: Configuration): Unit = {
    val driver:String = "com.mysql.jdbc.Driver"
    val url:String = "jdbc:mysql://hadoop01:3306/test"
    val username:String = "root"
    val password:String = "root"

    try {
      Class.forName(driver)
      //获取连接并赋值
      conn = DriverManager.getConnection(url, username, password)
      //将结果数据写入到mysql中
      val sql:String = "replace into gp1902_yq(dt,province,adds,possibles) values(?,?,?,?)"
      ps = conn.prepareStatement(sql)
    } catch {
      case e:SQLException => e.printStackTrace()
    }
  }

  //每条记录执行一次
  override def invoke(value: YQ, context: SinkFunction.Context[_]): Unit = {
    //将record中的数据进行赋值
    ps.setString(1,value.dt)
    ps.setString(2,value.province)
    ps.setInt(3,value.adds)
    ps.setInt(4,value.possibles)
    //触发
    ps.executeUpdate()
  }

  //关闭
  override def close(): Unit = {
    if(rs != null){
      rs.close()
    }
    if(ps != null){
      ps.close()
    }
    if(conn != null){
      conn.close()
    }
  }
}
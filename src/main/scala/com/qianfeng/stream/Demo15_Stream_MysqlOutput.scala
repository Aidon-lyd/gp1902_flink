package com.qianfeng.stream

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
 * 输入疫情数据：
 * date province add possible
 * 2020-06-10 zhejiang 120 230
 * 2020-06-10 zhejiang 12 23
 *
 * 输出:实时输出到mysql中---采用自定义output（根据date和province）
 * 2020-06-10 zhejiang 132 253
 */
object Demo15_Stream_MysqlOutput {
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

    //自定义mysql的outputformat输出
    res.writeUsingOutputFormat(new MyMysqlOutputFormat)

    //触发执行
    env.execute("connect")
  }
}

//封装类
case class YQ(dt:String,province:String,adds:Int,possibles:Int)

/*
自定义outputformat
1、需要继承OutputFormat
2、OutputFormat的泛型是输入流中的类型
 */
class MyMysqlOutputFormat extends OutputFormat[YQ]{
  //mysql的连接对象初始化
  var conn:Connection = _
  var ps:PreparedStatement = _
  var rs: ResultSet = _

  //进行配置 --- 对文件系统 --- 必须实现
  override def configure(parameters: Configuration): Unit = {

  }
  //初始化
  override def open(taskNumber: Int, numTasks: Int): Unit = {
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

  //每条数据执行一次，就是将record将其写出存储 ---- 1、批次计算 2、连接使用连接池
  override def writeRecord(record: YQ): Unit = {
    //将record中的数据进行赋值
    ps.setString(1,record.dt)
    ps.setString(2,record.province)
    ps.setInt(3,record.adds)
    ps.setInt(4,record.possibles)
    //触发
    ps.executeUpdate()
    //ps.executeBatch()
  }

  //结束关闭
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
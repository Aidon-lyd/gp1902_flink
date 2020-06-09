package com.qianfeng.stream

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * 自定义msyql的source
 */
object Demo04_Stream_MysqlSource {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2、自定义sourcefunction

    val dstream1: DataStream[String] = env.addSource(new MysqlRichParallelFunction).setParallelism(1)
    dstream1.print("mysql source->")

    //5、触发执行
    env.execute("mysql source")
  }
}

//自定义读取msyql中数据
class MysqlRichParallelFunction extends RichParallelSourceFunction[String]{

  var conn:Connection = _
  var ps:PreparedStatement = _
  var rs: ResultSet = _

  //初始化mysql的连接
  override def open(parameters: Configuration): Unit = {
    val driver:String = "com.mysql.jdbc.Driver"
    val url:String = "jdbc:mysql://hadoop01:3306/test"
    val username:String = "root"
    val password:String = "root"

    try {
      Class.forName(driver)
      //获取连接并赋值
      conn = DriverManager.getConnection(url, username, password)

      //获取数据
      val sql:String = "select * from stu1"
      ps = conn.prepareStatement(sql)
    } catch {
      case e:SQLException => e.printStackTrace()
    }
  }

  //读取mysql中的数据，然后发到下游中
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    //获取ps中的数据
    rs = ps.executeQuery()
    //取值
    while (rs.next()){
      //输出到下游
      ctx.collect(rs.getString(1)+"_"+rs.getString(2))
    }
  }

  override def cancel(): Unit = ???

  //关闭mysql的连接
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
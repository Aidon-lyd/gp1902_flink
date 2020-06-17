package com.qianfeng.stream

import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/*
cep案例：
1、过滤事件流规则为：start开始--->任意--->middle --->任意--->最后end
2、过滤事件流规则为：start开始--->middle --->任意--->最后end
3、过滤事件流规则为：start开始--->任意--->middle(3次) --->任意--->最后end
 */
object Demo39_stream_cep {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //②通过fromElements的方式准备一个DataStream
    import org.apache.flink.api.scala._
    val ds: DataStream[Event] = env.fromElements[Event](
      new Event(1, "start", 1.0), //理解为→ 登陆旅游网站
      new Event(2, "middle", 2.0), //理解为→ 浏览旅游网站上的旅游景点的信息
      new Event(3, "foobar", 3.0),
      new SubEvent(4, "foo", 4.0, 1.0),
      new Event(5, "middle", 5.0),
      new SubEvent(6, "middle", 6.0, 2.0),
      new SubEvent(7, "bar", 3.0, 3.0),
      new Event(42, "42", 42.0),
      new Event(8, "end", 1.0) //理解为→ 退出旅游网站
    )

    //③定制匹配模式规则 --- scala包
    /*
    //宽松近邻
    val patternRule = Pattern.begin[Event]("start")
      .where(_.getName.equals("start"))
      .followedByAny("middle")
      .where(_.getName.equals("middle")) //浏览
      .followedByAny("end") //退出
      .where(_.getName.equals("end"))*/

    //严格近邻
    /*val patternRule = Pattern.begin[Event]("start")
      .where(_.getName.equals("start"))
      .next("middle")
      .where(_.getName.equals("middle")) //浏览
      .followedByAny("end") //退出
      .where(_.getName.equals("end"))*/

    val patternRule = Pattern.begin[Event]("start")
      .where(_.getName.equals("start"))
      .followedBy("middle")
      .where(_.getName.equals("middle"))
      .followedBy("end") //退出
      .where(_.getName.equals("end"))

    /*val patternRule = Pattern.begin[Event]("start")
      .where(_.getName.equals("start"))
      .followedByAny("middle")
      .where(_.getName.equals("middle"))
      .where(_.getPrice > 5.0)
      //.times(4)  //匹配某一个出现的次数
      .followedByAny("end") //退出
      .where(_.getName.equals("end"))
      .within(Time.seconds(10))  //指定窗口，，希望10s中内配如上的规则，如果超过10s将不会进行匹配
*/
    //④将匹配模式应用于当前的DataStream中，从DataStream中筛选出满足条件的元素，放到匹配模式流中存储起来
    val ps: PatternStream[Event] = CEP.pattern(ds, patternRule)

    //⑤从匹配模式流中取出数据，并予以显示
    ps.flatSelect[String]((ele: scala.collection.Map[String, Iterable[Event]], out: Collector[String]) => {
      //步骤：
      //①准备一个容器，如：StringBuilder，用于存放结果
      val builder = new StringBuilder

      //②从匹配模式流中取出对应的元素,并追加到容器中
      val startEvent = ele.get("start").get.toList.head.toString
      val middleEvent = ele.get("middle").get.toList.head.toString
      val endEvent = ele.get("end").get.toList.head.toString

      builder.append(startEvent).append("\t|\t")
        .append(middleEvent).append("\t|\t")
        .append(endEvent)

      //③将结果通过Collector发送到新的DataStream中存储起来
      out.collect(builder.toString)

    }).print("CEP 运行结果 ： ")


    //⑥启动应用
    env.execute("cep")
  }
}

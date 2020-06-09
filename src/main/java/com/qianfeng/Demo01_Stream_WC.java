package com.qianfeng;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
*@Author 东哥
*@Company 千锋好程序员大数据
*@Date
*@Description  java版本的flink流式词频统计
**/
public class Demo01_Stream_WC {
    public static void main(String[] args) throws Exception {
        //1、获取执行环境变量
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2、添加source
        DataStreamSource<String> dstream = env.socketTextStream("hadoop01", 6666);

        //3、对source获取的流进行转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = dstream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] fields = value.split(" ");
                        for (String word : fields) {
                            //输出
                            out.collect(new Tuple2<String, Integer>(word, 1));
                        }
                    }
                })
                .keyBy(0)
                .sum(1);

        //4、将转换流sink持久化
        res.print("java sc");

        //5、触发执行
        env.execute("java-stream-wc");
    }
}

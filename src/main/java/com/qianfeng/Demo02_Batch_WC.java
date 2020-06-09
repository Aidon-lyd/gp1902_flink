package com.qianfeng;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
*@Author 东哥
*@Company 千锋好程序员大数据
*@Date java版本的批次词频统计
*@Description
**/
public class Demo02_Batch_WC {
    public static void main(String[] args) throws Exception {
        //1、获取执行环境变量
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2、获取source
        DataSource<String> dset = env.fromElements("hello qianfeng hello hello qianfeng");

        //3、转换
        AggregateOperator<Tuple2<String, Integer>> res = dset.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] fields = value.split(" ");
                for (String word : fields) {
                    //输出
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        })
                .groupBy(0)
                .sum(1);

        //4、添加sink
        res.print();
    }
}

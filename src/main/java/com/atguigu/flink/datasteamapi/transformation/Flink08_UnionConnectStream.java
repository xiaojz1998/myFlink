package com.atguigu.flink.datasteamapi.transformation;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 合流:
 *  1. union  将多条流合并成一条流，要求被合并的流中的数据类型必须一致。
 *  2. connect  将两条流合并成一条流， 支持流中的类型可以不一样。 合并后，需要将类型进行统一。
 */
public class Flink08_UnionConnectStream {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 准备几条流，用集合方法 ,这里的流要用不同的种类
        DataStreamSource<String> ds1 = env.fromElements("a", "b", "c", "d", "e");
        DataStreamSource<String> ds2 = env.fromElements("hello", "world", "flink", "spark");
        DataStreamSource<Integer> ds3 = env.fromElements(1, 2, 3, 4, 5);

        // union 合流
        DataStream<String> unionDs = ds1.union(ds2, ds3.map(Object::toString));
        //unionDs.print("Union");

        // connect 合流
        // connect只是对两个流暂时合并，但并没有统一类型
        ConnectedStreams<String, Integer> connectDs = ds1.connect(ds3);
        // 对两个流同一类型
        SingleOutputStreamOperator<String> processDs= connectDs.process(
                new CoProcessFunction<String, Integer, String>() {
                    // 对第一个元素处理
                    @Override
                    public void processElement1(String s, CoProcessFunction<String, Integer, String>.Context context, Collector<String> collector) throws Exception {
                        collector.collect(s);
                    }

                    // 对第二个元素处理
                    @Override
                    public void processElement2(Integer integer, CoProcessFunction<String, Integer, String>.Context context, Collector<String> collector) throws Exception {
                        collector.collect(integer.toString());
                    }
                }
        );

        processDs.print("Connect");


        try {
            env.execute();
        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }
}

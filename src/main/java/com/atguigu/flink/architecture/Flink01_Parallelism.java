package com.atguigu.flink.architecture;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *
 * 并行度:  并行执行的程序(数量)
 *   1. 算子并行度
 *       一个算子在执行时并行实例（Task）的个数
 *   2. 作业并行度
 *        作业中并行度最大的算子的并行度
 *   3. 设置并行度
 *      1） 在IDEA中执行， 没有明确设置并行度的情况下，默认的并行度为当前电脑CPU的逻辑处理器个数
 *      2） 在代码中设置全局并行度
 *         env.setParallelism(1);
 *				当只有全局并行度的时候 为1
 *                       total task slots为1（因为有合并） task 为2 如下图
 当只有全局并行度的时候 为4
 Total Task Slots为4（因为有合并）Tasks 为9如下图
 *      3) 在代码中给每个算子设置并行度
 *               当各个算子设置并行度后全局并行度失效
 *                        total task slots为4(和最大的相同)task为10(1+2+3+4)  如下图
 *      4) 集群默认的并行度 : parallelism.default: 1
 *      5) 提交作业的时候，通过参数动态指定并行度
 *         flink run -p 并行度数量 ...
 *          bin/flink run –p 2 –c com.atguigu.wc.SocketStreamWordCount
 *           ./FlinkTutorial-1.0-SNAPSHOT.jar
 *
 */
public class Flink01_Parallelism {
    public static void main(String[] args) {
        // web设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 5678);

        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度
        env.setParallelism(1);
        // 2. 从数据源读取数据
        DataStreamSource<String> ds = env.socketTextStream("hadoop102", 8888);

        // 切分、分组、统计
        SingleOutputStreamOperator<Tuple2<String, Long>> sumDs = ds.flatMap(
                        new FlatMapFunction<String, Tuple2<String, Long>>() {
                            @Override
                            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                                String[] s1 = s.split(" ");
                                for (String word : s1) {
                                    collector.collect(Tuple2.of(word, 1L));
                                }
                            }
                        }
                ).setParallelism(2)
                .keyBy(a -> a.f0)
                .sum(1).setParallelism(3);;

        sumDs.print().setParallelism(4);

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

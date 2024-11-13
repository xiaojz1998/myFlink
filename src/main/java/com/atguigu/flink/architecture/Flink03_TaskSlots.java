package com.atguigu.flink.architecture;

import com.atguigu.flink.pojo.WordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *
 * 任务槽共享:  Flink允许上下游算子的并行实例共享同一个Slot
 * 为什么共享?
 *   1) 一个 slot 可以持有整个作业管道 , 即使某个TaskManager出现故障宕机，其他节点也可以完全不受影响，作业的任务可以继续执行。
 *   2) Flink 集群所需的 task slot 和作业中使用的最大并行度恰好一样。无需计算程序总共包含多少个 task
 *   3) 容易获得更好的资源利用。如果没有 slot 共享，非密集 subtask（source/map()）将阻塞和密集型 subtask（window） 一样多的资源。
 * 可不可以不共享？
 *   通过设置共享组来实现共享与不共享。
 *   默认情况下， 从source算子开始，设置默认的共享组为Default， 后续的算如果不明确指定共享组的情况下，从上游算子继承共享组
 */
public class Flink03_TaskSlots {

    public static void main(String[] args) {
        // web ui
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 5678);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        env.socketTextStream("hadoop102" , 8888)
                .flatMap(
                        new FlatMapFunction<String, Tuple2<String,Long>>() {
                            @Override
                            public void flatMap(String value, Collector<Tuple2<String,Long>> out) throws Exception {
                                String[] words = value.split(" ");
                                for (String word : words) {
                                    out.collect(Tuple2.of(word,1L));
                                }
                            }
                        }
                ) //.slotSharingGroup("g1")
                .keyBy(
                        v -> v.f0
                ).sum(1)  //.slotSharingGroup("g2")
                .print()  ;//.slotSharingGroup("g3") ;

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

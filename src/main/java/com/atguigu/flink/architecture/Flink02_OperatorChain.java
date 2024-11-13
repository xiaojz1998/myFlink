package com.atguigu.flink.architecture;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 算子链:
 *  1. Flink的分区规则（数据分发规则）
 *       ChannelSelector
 *          RebalancePartitioner        :  绝对负载均衡， 通过轮询的方式给下游算子的并行实例发送数据。
 *                                         上下游算子的并行度不一致，默认的分发规则就是 REBALANCE
 *                                         通过 .rebalance() 来明确指定分发规则
 *          RescalePartitioner          :  相对负载均衡，通过轮询的方式给下游组内的算子的并行实例发送数据。
 *                                         通过 .rescale() 来明确指定分发规则
 *          KeyGroupStreamPartitioner   : 按 key 分组 ， 通过key的hash值对下游算子的并行度取余得到要发送的并行实例
 *                                        通过 .keyBy() 来明确指定分发规则
 *          ShufflePartitioner          : 随机， 通过随机的方式给下游算子的并行实例发送数据
 *                                        通过  .shuffle()来明确指定分区规则
 *          BroadcastPartitioner        : 广播，通过广播的方式给下游算子的并行实例发送数据
 *                                        通过 .broadcast() 来明确指定分区规则
 *          GlobalPartitioner           : 全局，只给下游算子的第一个并行实例发送数据， 强制并行度为1
 *          ForwardPartitioner          : 直连， 上下游的并行度必须一样， 上下游的并行实例一一对应。
 *                                        如果上下游算子的并行度一样， 默认的分区规则就是 FORWARD
 * 2. 算子链:  将满足条件的上下游的算子的并行实例合并到一起，形成算子链。
 *    1） 合并算子链要满足的条件:
 *          上下游的算子的并行度一样  且 数据的分发规则为 forward
 *    2) 为什么要合并算子链？
 *          它减少线程间切换、缓冲的开销，并且减少延迟的同时增加整体吞吐量
 *    3） 能不能不合并？
 *          ① 全局禁用算子链
 *              env.disableOperatorChaining();
 *          ② 设置某个算子是否不参与合并
 *              算子.startNewChain()    不与上游的算子合并。
 *              算子.disableChaining()  不与上有和下游的算子合并。
 */
public class Flink02_OperatorChain {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",5678);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        // 全局禁用算子链条
        //env.disableOperatorChaining();
        env.socketTextStream("hadoop102", 8888)
                .rebalance()
                //.keyBy(v->v)
                //.shuffle()
                //.broadcast()
                //.global()
                //.forward()
                //.rebalance()
                .map(v->v).name("m1").setParallelism(2)//.disableChaining() // .startNewChain()
               //.rescale()
                .map(v->v).name("m2").setParallelism(2)
                .print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

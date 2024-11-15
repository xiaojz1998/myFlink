package com.atguigu.flink.datasteamapi.transformation;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 1. Flink的分区规则（数据分发规则）
 *      ChannelSelector
 *         RebalancePartitioner        :  绝对负载均衡， 通过轮询的方式给下游算子的并行实例发送数据。
 *                                        上下游算子的并行度不一致，默认的分发规则就是 REBALANCE
 *                                        通过 .rebalance() 来明确指定分发规则
 *
 *         RescalePartitioner          :  相对负载均衡，通过轮询的方式给下游组内的算子的并行实例发送数据。
 *                                        通过 .rescale() 来明确指定分发规则
 *
 *         KeyGroupStreamPartitioner   : 按 key 分组 ， 通过key的hash值对下游算子的并行度取余得到要发送的并行实例
 *                                       通过 .keyBy() 来明确指定分发规则
 *
 *         ShufflePartitioner          : 随机， 通过随机的方式给下游算子的并行实例发送数据
 *                                       通过  .shuffle()来明确指定分区规则
 *
 *         BroadcastPartitioner        : 广播，通过广播的方式给下游算子的并行实例发送数据
 *                                       通过 .broadcast() 来明确指定分区规则
 *
 *         GlobalPartitioner           : 全局，只给下游算子的第一个并行实例发送数据， 强制并行度为1
 *
 *         ForwardPartitioner          : 直连， 上下游的并行度必须一样， 上下游的并行实例一一对应。
 *                                       如果上下游算子的并行度一样， 默认的分区规则就是 FORWARD
 *
 *         自定义分区 ， 通过KeySelector来确定数据中的key ， 再通过Partitioner来计算key的分区号
 *                     下游算子的并行度要大于等于上游自定义的分区数。
 */
public class Flink04_PartitionerOperator {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> ds = env.fromSource(SourceUtil.getSource(), WatermarkStrategy.noWatermarks(), "Mysource");
        ds.print("INPUT");
        //需求: 将"Tom" 和 "Jerry" 分到0好区 ，其他的分到1号区
        DataStream<Event> partitionDs = ds.partitionCustom(
                // partioner中的泛型是key的泛型，就是你要分区的参数，而不是传入的参数
                // 传入参数在后面得到
                new Partitioner<String>() {
                    @Override
                    // 第一个参数是分区的参数，第二个参数是后面的分区数量，由后面算子传过来
                    public int partition(String s, int i) {
                        if ("Tom".equals(s) || "Jerry".equals(s)) {
                            return 0;
                        }
                        return 1;
                    }
                },
                new KeySelector<Event, String>() {
                    @Override
                    public String getKey(Event event) throws Exception {
                        return event.getUser();
                    }
                }
        );
        // 因为前面是分区器，所以这个接收算子的分区要比分区器的并行度大
        // 否则会报数组下标越界异常的错误
        partitionDs.print("PARTITION").setParallelism(2);

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

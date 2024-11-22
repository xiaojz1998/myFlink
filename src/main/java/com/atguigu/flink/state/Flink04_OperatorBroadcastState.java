package com.atguigu.flink.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

// 广播状态
// 用处 用于实现以下类似的需求：
//    1. 实现动态更新配置算子参数和逻辑
//          通过广播一个标记， 让算子的每个并行实例按照标记执行不同的逻辑代码
public class Flink04_OperatorBroadcastState {
    public static void main(String[] args) {
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.setParallelism(1);
         //需求: 通过广播一个标记， 让算子的每个并行实例按照标记执行不同的逻辑代码

        // 数据流
        DataStreamSource<String> ds = env.socketTextStream("hadoop102", 8888);
        // 广播流
        // 基于普通流进行广播操作得到广播流
        // 广播状态是一个字典，kv对，用于配置算子
        MapStateDescriptor<String, String> mapStateDesc =new MapStateDescriptor<>("mapStateDesc", Types.STRING,Types.STRING);
        BroadcastStream<String> broadcastDs = env.socketTextStream("hadoop102", 9999).broadcast(mapStateDesc);

        // connect
        ds.connect(broadcastDs)
                .process(
                        new BroadcastProcessFunction<String, String, String>() {
                            /**
                             * 处理数据流中的数据
                             * 从广播状态中获取状态数据， 结合上状态数据，对数据流中的数据进行处理。
                             * 对广播状态进行读取
                             */
                            @Override
                            public void processElement(String s, BroadcastProcessFunction<String, String, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
                                ReadOnlyBroadcastState<String, String> broadcastState = readOnlyContext.getBroadcastState(mapStateDesc);
                                String key = broadcastState.get("key");
                                if("1".equals(key)){
                                    System.out.println("执行1号逻辑处理....");
                                }else if("2".equals(key)){
                                    System.out.println("执行2号处理逻辑....");
                                }else if("3".equals(key)){
                                    System.out.println("执行3号处理逻辑....");
                                }else{
                                    System.out.println("执行默认处理逻辑....");
                                }
                                collector.collect(s);
                            }
                            /**
                             * 处理广播流中的数据
                             * 广播流中进来数据，将数据添加到广播状态中。
                             * 对广播状态进行 新增 、 修改 、 删除
                             */
                            @Override
                            public void processBroadcastElement(String s, BroadcastProcessFunction<String, String, String>.Context context, Collector<String> collector) throws Exception {
                                // 获取广播状态
                                // 广播状态是一个字典，kv对，用于配置算子
                                BroadcastState<String, String> broadcastState = context.getBroadcastState(mapStateDesc);
                                // 将广播的数据添加到广播状态中
                                broadcastState.put("key", s);
                            }
                        }
                ).setParallelism(2).print();


        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

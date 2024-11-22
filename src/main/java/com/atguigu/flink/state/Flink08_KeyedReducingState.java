package com.atguigu.flink.state;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 按键分区状态 - 归约状态 - ReducingState
 *
 *  ReducingState:  单值状态，不过有归约功能， 会按照定义好的归约逻辑对存入到状态中的数据进行归约处理。
 *      OUT get():  获取状态值
 *      void add(IN value) : 将给定的数据添加到状态中
 *      void clear():  清空状态
 *
 */
public class Flink08_KeyedReducingState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // s1,100,1000
        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new WaterSensor(fields[0].trim(), Integer.valueOf(fields[1].trim()), Long.valueOf(fields[2].trim()));
                        }
                );

        ds.print("INPUT");

        // 需求: 计算每种传感器的水位和
        ds.keyBy(WaterSensor::getId)
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            // 1. 声明状态
                            private ReducingState<Integer> vcSumState;
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                RuntimeContext runtimeContext = getRuntimeContext();
                                ReducingStateDescriptor<Integer> vcSumStateDesc = new ReducingStateDescriptor<>("vcSumState",
                                        new ReduceFunction<Integer>() {
                                            @Override
                                            public Integer reduce(Integer integer, Integer t1) throws Exception {
                                                return integer + t1;
                                            }
                                        },
                                        Types.INT);
                                vcSumState = runtimeContext.getReducingState(vcSumStateDesc);
                            }
                            @Override
                            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                                // 使用状态
                                vcSumState.add(waterSensor.getVc());
                                collector.collect("当前传感器："+waterSensor.getId()+"，当前水位和是："+vcSumState.get());
                            }
                        }
                ).print("PROCESS");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}

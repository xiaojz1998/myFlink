package com.atguigu.flink.state;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


import java.time.Duration;

/**
 * 按键分区状态 - 值状态 - ValueState
 * ValueState: 单值状态， 状态中只会维护一个值
 *     T value():  获取状态中的值
 *     void update(T value): 更新状态中的值
 *     void clear(): 清空状态
 */
public class Flink05_KeyedValueState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 控制台输入值格式s1,100,1000
        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(
                        line -> {
                            String[] split = line.split(",");
                            return new WaterSensor(split[0], Integer.valueOf(split[1].trim()), Long.valueOf(split[2].trim()));
                        }
                ).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        (e, ts) -> e.getTs()
                                )
                );

        ds.print("INPUT");
        // 需求: 检测每种传感器的水位值，如果连续的两个水位值相差超过10，就输出报警。

        ds.keyBy(WaterSensor::getId)
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            // 1. 声明状态
                            private ValueState<Integer> lastVcState;
                            // 2. 使用生命
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                RuntimeContext runtimeContext = getRuntimeContext();
                                ValueStateDescriptor<Integer> lastvaluestate = new ValueStateDescriptor<>("lastvaluestate" , Types.INT);
                                lastVcState = runtimeContext.getState(lastvaluestate);
                            }
                            @Override
                            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                                // 3. 使用状态
                                // 从状态中获取上一次的水位值
                                Integer lastvc = lastVcState.value();
                                if(lastvc!=null && Math.abs(waterSensor.getVc()-lastvc)>10){
                                    collector.collect("当前水位值与上一次的水位值相差超过10，当前水位值是："+waterSensor.getVc()+"，上一次的水位值是："+lastvc);
                                }
                                // 更新状态
                                lastVcState.update(waterSensor.getVc());
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

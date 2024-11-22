package com.atguigu.flink.state;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 按键分区状态 - 聚合状态 - AggregatingState
 *  AggregatingState:  单值状态，不过有聚合功能， 会按照定义好的聚合逻辑对存入到状态中的数据进行聚合处理。
 *      OUT get():  获取状态值
 *      void add(IN value) : 将给定的数据添加到状态中
 *      void clear():  清空状态
 */
public class Flink09_KeyedAggregatingState {
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

        // 需求: 计算每种传感器的平均水位
        ds.keyBy(WaterSensor::getId)
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            private AggregatingState<Integer, Double> avgVcState;
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                RuntimeContext runtimeContext = getRuntimeContext();
                                AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double> myA = new AggregatingStateDescriptor<>("myA",
                                        new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                            @Override
                                            public Tuple2<Integer, Integer> createAccumulator() {
                                                return Tuple2.of(0, 0);
                                            }

                                            @Override
                                            public Tuple2<Integer, Integer> add(Integer integer, Tuple2<Integer, Integer> integerIntegerTuple2) {
                                                return Tuple2.of(integerIntegerTuple2.f0 + integer, integerIntegerTuple2.f1 + 1);
                                            }

                                            @Override
                                            public Double getResult(Tuple2<Integer, Integer> integerIntegerTuple2) {
                                                return (double) integerIntegerTuple2.f0 / integerIntegerTuple2.f1;
                                            }

                                            @Override
                                            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> acc1) {
                                                return null;
                                            }
                                        },
                                        Types.TUPLE(Types.INT, Types.INT));
                                avgVcState = runtimeContext.getAggregatingState(myA);
                            }

                            @Override
                            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                                // 3. 使用状态
                                avgVcState.add(waterSensor.getVc());
                                // 输出
                                collector.collect(waterSensor.getId()+"的平均水位是："+avgVcState.get());
                            }
                        }
                ).print("process");
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}

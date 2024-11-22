package com.atguigu.flink.state;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;

/**
 *
 * 按键分区状态 - 列表状态 - ListState
 *  ListState:  可以维护多个状态值
 *     update(List<T> values) :   将给定的list集合中的数据覆盖掉状态中已有的值
 *     void addAll(List<T> values) : 将给定的list集合中的数据添加到状态中
 *     OUT get():  获取状态中的值
 *     void add(IN value): 将给定的数据添加到状态中
 *     void clear(): 清空状态
 *
 */
public class Flink06_KeyedListState {
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
        // 需求: 针对每种传感器输出最高的3个水位值
        ds.keyBy(WaterSensor::getId)
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            // 声明变量当做状态
                            private ListState<Integer> listState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                // 在open里面初始化状态
                                RuntimeContext runtimeContext = getRuntimeContext();
                                ListStateDescriptor<Integer> myListState = new ListStateDescriptor<>("myListState", Types.INT);
                                // 这里是getListState
                                listState = runtimeContext.getListState(myListState);
                            }

                            @Override
                            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                                // 3. 使用状态
                                // 将当前的水位值添加到状态中
                                listState.add(waterSensor.getVc());
                                // 取出状态的值
                                ArrayList<Integer> l = new ArrayList<>();
                                listState.get().forEach(l::add);
                                // 对l进行排序
                                l.sort((a,b)->Integer.compare(b,a));
                                if(l.size()>3) l.remove(3);
                                // 更新状态
                                listState.update(l);
                                // 输出
                                collector.collect("当前传感器："+waterSensor.getId()+"，当前水位值是："+waterSensor.getVc()+"，当前最高的3个水位值是："+ Arrays.toString(l.toArray()));
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

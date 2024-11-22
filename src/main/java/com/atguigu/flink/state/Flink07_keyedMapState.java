package com.atguigu.flink.state;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 按键分区状态 - Map状态 - MapState
 *  MapState:  可以维护多个状态值 , 值的格式为kv对
 *     UV get(UK key)： 按照指定的key获取对应的value
 *     void put(UK key, UV value)： 将指定的key和value添加到状态中
 *     void putAll(Map<UK, UV> map)：将指定的map集合中的kv添加到状态中
 *     void remove(UK key)： 按照指定的key移除对应的value
 *     boolean contains(UK key)： 判断是否包含指定的key
 *     Iterable<Map.Entry<UK, UV>> entries()：获取所有的kv对
 *     Iterable<UK> keys()： 获取所有的key
 *     Iterable<UV> values()： 获取所有的value
 *     Iterator<Map.Entry<UK, UV>> iterator()： 获取迭代器对象
 *     boolean isEmpty()： 判断是否为空
 *     void clear()： 清空状态
 */
public class Flink07_keyedMapState {
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

        // 需求: 去掉每种传感器重复的水位值。（记录每种传感器上报的所有的水位值， 重复的水位值进行去重操作）
        ds.keyBy(WaterSensor::getId)
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            // 声明变量
                            private MapState<Integer, Integer> mapState;
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                // 初始化状态
                                RuntimeContext runtimeContext = getRuntimeContext();
                                MapStateDescriptor<Integer, Integer> mapStateDes = new MapStateDescriptor<>("mapState", Integer.class, Integer.class);
                                mapState = runtimeContext.getMapState(mapStateDes);

                            }

                            @Override
                            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                                // 使用状态
                                mapState.put(waterSensor.getVc(), null);
                                Iterable<Integer> keys = mapState.keys();
                                // 输出
                                collector.collect("传感器: " + context.getCurrentKey() + " , 去重后的水位值: " + keys );
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

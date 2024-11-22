package com.atguigu.flink.state;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * 状态的TTL - Time To Live
 * 给状态设置过期时间， 在指定的时间内，没有对状态进行操作( 读 或者 写) ， 状态会被清理，
 * 如果在指定的时间内，对状态进行了操作， 时间会重置。
 */
public class Flink10_StateTTL {
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

        // 需求: 针对每种传感器输出最高的3个水位值
        ds.keyBy(
                WaterSensor::getId
        ).process(
                new KeyedProcessFunction<String, WaterSensor, String>() {

                    // 1. 声明状态
                    private ListState<Integer> top3VcState ;

                    // 2. 使用生命周期open方法， 初始化状态
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        RuntimeContext runtimeContext = getRuntimeContext();
                        ListStateDescriptor<Integer> top3VcStateDesc = new ListStateDescriptor<>("top3VcStateDesc", Types.INT);
                        // TTL
                        StateTtlConfig stateTtlConfig =
                                StateTtlConfig.newBuilder(Time.seconds(10))
                                        .setUpdateType( StateTtlConfig.UpdateType.OnReadAndWrite)
                                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                        .build();
                        top3VcStateDesc.enableTimeToLive(stateTtlConfig);

                        top3VcState = runtimeContext.getListState(top3VcStateDesc);
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        //3. 使用状态
                        // 将当前的水位值添加到状态中
                        top3VcState.add( value.getVc() );

                        // 取出状态的值
                        ArrayList<Integer> stateVcList = new ArrayList<Integer>();
                        top3VcState.get().forEach(stateVcList::add);

                        // 排序
                        stateVcList.sort(
                                //new Comparator<Integer>() {
                                //    @Override
                                //    public int compare(Integer o1, Integer o2) {
                                //        return Integer.compare(o2, o1) ;
                                //    }
                                //}
                                (o1, o2) -> Integer.compare(o2, o1)
                        );

                        // 取前3个
                        if(stateVcList.size() > 3 ){
                            //移除最后一个
                            stateVcList.remove(3);
                        }

                        //更新状态
                        top3VcState.update( stateVcList );

                        //输出
                        out.collect("传感器: " + ctx.getCurrentKey() + " , 最高的3个水位: " + stateVcList  );
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

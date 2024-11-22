package com.atguigu.flink.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 *  原始状态
 *      为什么我们不建议使用原始状态？
 *          因为原始状态在报错并且重启后，会导致报错前的状态丢失
 */
public class Flink01_RawState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 开启检查点，程序失败后会无线重启
        env.enableCheckpointing(2000L);
        // 设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(2)));

        DataStreamSource<String> ds = env.socketTextStream("hadoop102", 8888);
        // 需求: 通过map算子记录输入的每个数据
        ds.map(new RawStateMapFunction())
                .addSink(new myPrintSinkFunction());

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public static class myPrintSinkFunction implements SinkFunction<String> {
        @Override
        public void invoke(String value, Context context) throws Exception {
            if( value.contains("x")){
                //模拟异常
                throw new RuntimeException("抛出了异常....");
            }

            System.out.println("MYPRINT> " + value );
        }
    }
    public static class RawStateMapFunction implements MapFunction<String,String>{
        //定义一个集合，记录map中的所有输入数据
        //原始状态
        List<String> dataList = new ArrayList<String>() ;
        @Override
        public String map(String s) throws Exception {
            dataList.add(s);
            return dataList.toString();
        }
    }
}

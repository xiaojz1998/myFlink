package com.atguigu.flink.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * 算子状态 - 列表状态
 */
public class Flink02_OperatorListState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 开启检查点，程序失败后会无线重启
        env.enableCheckpointing(2000L);
        // 设置重启策略
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(2)));

        DataStreamSource<String> ds = env.socketTextStream("hadoop102", 8888);
        // 需求: 通过map算子记录输入的每个数据
        ds.map(new RawStateMapFunction()).setParallelism(2)
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
    // 要用列表状态必须实现CheckpointedFunction
    public static class RawStateMapFunction implements MapFunction<String,String> , CheckpointedFunction {
        // 定义一个集合，记录map中的所有输入数据
        // 列表状态
        private ListState<String> listState ;
        @Override
        public String map(String s) throws Exception {
            listState.add(s);
            return listState.toString();
        }
        /**
         * 对状态进行持久化存储， 基于检查点机制，
         * 周期性执行， 按照检查点的周期来调用。
         */
        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            System.out.println("snapshotState......");
        }
        /**
         * 初始化状态， 状态的获取  以及  程序重启时状态的还原
         * 程序启动时会调用一次
         */
        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
            System.out.println("initializeState......");
            // 初始化状态
            OperatorStateStore operatorStateStore = functionInitializationContext.getOperatorStateStore();
            ListStateDescriptor<String> listStateDesc = new ListStateDescriptor<>("listStateDesc", Types.STRING);
            listState = operatorStateStore.getListState(listStateDesc);
        }
    }
}

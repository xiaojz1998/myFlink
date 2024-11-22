package com.atguigu.flink.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * 算子状态 - 联合列表状态
 * 列表状态和 联合列表状态的区别:
 *    从使用上来说。两者没有太大区别， 本质上都是列表状态。
 *    当程序重启，还原状态的时候， 列表状态还原后的状态与程序失败前保持一致。  联合列表状态还原后的状态是 将失败前每个并行实例的状态联合到一起给到每个并行实例。
 * 联合列表状态的应用场景:   KafkaSource
 *    失败前:
 *            Source       并行实例     消费的分区    offset
 *         KafkaSource  ->   P0    ->    P0    ->   100
 *         KafkaSource  ->   P1    ->    P1    ->   101
 *         KafkaSource  ->   P2    ->    P2    ->   99
 *         KafkaSource  ->   P3    ->    P3    ->   102
 *    失败重启后:
 *            Source       并行实例     消费的分区     offset
 *          KafkaSource  ->   P0    ->    P1    ->   [P0 -> 100 , P1 -> 101  , P2 -> 99 , P3 -> 102 ]
            KafkaSource  ->   P1    ->    P3    ->   [P0 -> 100 , P1 -> 101  , P2 -> 99 , P3 -> 102 ]
            KafkaSource  ->   P2    ->    P0    ->   [P0 -> 100 , P1 -> 101  , P2 -> 99 , P3 -> 102 ]
            KafkaSource  ->   P3    ->    P2    ->   [P0 -> 100 , P1 -> 101  , P2 -> 99 , P3 -> 102 ]
 */
public class Flink03_OperatorUnionListState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 开启检查点
        env.enableCheckpointing(2000l);

        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3 , Time.seconds(2)));

        DataStreamSource<String> ds = env.socketTextStream("hadoop102", 8888);



    }
    public static class MyPrintSinkFunction  implements SinkFunction<String> {
        @Override
        public void invoke(String value, Context context) throws Exception {
            if( value.contains("x")){
                //模拟异常
                throw new RuntimeException("抛出了异常....");
            }

            System.out.println("MYPRINT> " + value );
        }
    }

    public static class OperatorUnionListStateMapFunction implements MapFunction<String,String> , CheckpointedFunction {

        // 定义一个集合
        private ListState<String> listState;
        @Override
        public String map(String s) throws Exception {
            // 王状态列表中添加数据
            listState.add(s);
            // 从状态中获取数据
            return  listState.get().toString();
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
            listState=operatorStateStore.getUnionListState(listStateDesc);
        }
    }
}

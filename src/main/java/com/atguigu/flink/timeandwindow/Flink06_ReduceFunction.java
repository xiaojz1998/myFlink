package com.atguigu.flink.timeandwindow;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 增量聚合函数:
 *   ReduceFunction ， 两两归约， 上一次的聚合结果和本次的数据进行聚合。 输入类型和输出类型保持一致。
 */
public class Flink06_ReduceFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1000);

        SingleOutputStreamOperator<Event> ds = env.fromSource(SourceUtil.getSource(), WatermarkStrategy.noWatermarks(), "MySource")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        (e, t) -> e.getTs()
                                )
                );
        ds.print("INPUT");
        // 需求: 统计10秒内的用户点击次数(pv)
        // 窗口:  非按键分区事件时间滚动窗口
        ds.map(event -> Tuple1.of(1L)).returns(Types.TUPLE(Types.LONG))
                .windowAll(
                        TumblingEventTimeWindows.of(Time.seconds(10))
                ).reduce(
                        new ReduceFunction<Tuple1<Long>>() {
                            @Override
                            public Tuple1<Long> reduce(Tuple1<Long> longTuple1, Tuple1<Long> t1) throws Exception {
                                return longTuple1.of(longTuple1.f0 + t1.f0);
                            }
                        }
                ).print("PV");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }



    }
}

package com.atguigu.flink.timeandwindow;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

/**
 *
 * 需求: 在电商网站中，PV（页面浏览量）和UV（独立访客数）是非常重要的两个流量指标。
 *     一般来说，PV统计的是所有的点击量；而对用户id进行去重之后，得到的就是UV。
 *      所以有时我们会用PV/UV这个比值，来表示“人均重复访问量”，也就是平均每个用户会访问多少次页面，这在一定程度上代表了用户的粘度。
 *      统计每10秒内的 PV/UV
 */
public class Flink08_PVUV {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1).getConfig().setAutoWatermarkInterval(1000);

        SingleOutputStreamOperator<Event> ds = env.fromSource(SourceUtil.getSource(), WatermarkStrategy.noWatermarks(), "source")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        (e, t) -> e.getTs()
                                )
                );

        ds.print("INPUT");

        ds.windowAll(
                TumblingEventTimeWindows.of(Time.seconds(8))
        ).aggregate(
                new AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Double>() {
                    @Override
                    public Tuple2<Long, HashSet<String>> createAccumulator() {
                        return Tuple2.of(0L,new HashSet<>());
                    }

                    @Override
                    public Tuple2<Long, HashSet<String>> add(Event event, Tuple2<Long, HashSet<String>> longHashSetTuple2) {
                        longHashSetTuple2.f0++;
                        longHashSetTuple2.f1.add(event.getUser());
                        return longHashSetTuple2;
                    }

                    @Override
                    public Double getResult(Tuple2<Long, HashSet<String>> longHashSetTuple2) {
                        return (double) longHashSetTuple2.f0/longHashSetTuple2.f1.size();
                    }

                    @Override
                    public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> longHashSetTuple2, Tuple2<Long, HashSet<String>> acc1) {
                        return null;
                    }
                }
        ).print("PV/UV");



        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

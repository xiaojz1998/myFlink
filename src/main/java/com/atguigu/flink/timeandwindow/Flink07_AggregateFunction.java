package com.atguigu.flink.timeandwindow;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

/**
 * 增量聚合函数:
 *   AggregateFunction ， 两两聚合。 支持输入和输出类型不一致。
 *     三个泛型:
 *        <IN> – The type of the values that are aggregated (input values)   输入数据类型
 *        <ACC> – The type of the accumulator (intermediate aggregate state).  累加器类型
 *        <OUT> – The type of the aggregated result  输出数据类型
 *
 *    四个方法:
 *         createAccumulator() :  创建累加器 , 只会调用一次
 *         add() : 进行累加操作 ， 每条数据调用一次
 *         getResult(): 获取结果 ， 只会调用一次
 *         merge(): 合并累加器
 */
public class Flink07_AggregateFunction {
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

        // 需求: 统计10秒内的UV（独立访客数）不能有重复，所以用set做中间值
        // 窗口: 非按键分区事件时间滚动窗口
        ds.windowAll(
                TumblingEventTimeWindows.of(Time.seconds(8))
        ).aggregate(
                // 使用hashSet维护所有访问的人， hashset有天然去重功能， 最后只要取hashset的长度既可表示UV
                // 三个泛型看上面注释
                new AggregateFunction<Event, HashSet<String>, Long>() {
                    @Override
                    public HashSet<String> createAccumulator() {
                        return new HashSet<String>();
                    }

                    @Override
                    public HashSet<String> add(Event event, HashSet<String> strings) {
                        System.out.println("add...");
                        strings.add(event.getUser());
                        return strings;
                    }

                    @Override
                    public Long getResult(HashSet<String> strings) {
                        return (long)strings.size();
                    }

                    @Override
                    public HashSet<String> merge(HashSet<String> strings, HashSet<String> acc1) {
                        return null;
                    }
                }
        ).print("UV");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

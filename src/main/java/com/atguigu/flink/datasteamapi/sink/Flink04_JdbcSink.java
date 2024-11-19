package com.atguigu.flink.datasteamapi.sink;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.WordCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class Flink04_JdbcSink {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> ds = env.socketTextStream("hadoop102", 8888)
                .map(
                        s -> {
                            String[] split = s.split(",");
                            return new Event(split[0], split[1], Long.parseLong(split[2]));
                        }
                ).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        (element, ts) -> element.getTs()
                                )
                );

        ds.print("INPUT");

        // 按键分区窗口
        // 计数窗口
        // 计数滚动窗口
        //ds.map(event -> new WordCount(event.getUser(), 1L))
        //        .keyBy(WordCount::getWord)
        //        .countWindow(3L)
        //        .sum("count")
        //        .print();

        // 计数滑动窗口
        //ds.map(event -> new WordCount(event.getUser(), 1L))
        //        .keyBy(WordCount::getWord)
        //        .countWindow(3L,2L)
        //        .sum("count")
        //        .print();

        // 时间窗口
        ds.map(event->{return new WordCount(event.getUser(),1L);})
                .keyBy(WordCount::getWord)
                .window(
                        // 时间滚动窗口
                        // 处理时间
                        //TumblingProcessingTimeWindows.of(Time.seconds(5))
                        // 事件时间
                        //TumblingEventTimeWindows.of(Time.seconds(5))

                        // 时间滑动窗口
                        // 处理时间
                        //SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))
                        // 事件时间
                        //SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5))

                        // 时间会话窗口
                        // 处理时间
                        //ProcessingTimeSessionWindows.withGap(Time.seconds(5))
                        // 事件时间
                        EventTimeSessionWindows.withGap(Time.seconds(5))

                        // 全局窗口
                        //GlobalWindows.create()
                ).sum("count")
                .print();
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

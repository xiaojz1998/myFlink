package com.atguigu.flink.timeandwindow;

import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 熟练使用窗口的前置结论：
 *   1. 数据落哪个窗口，看的是数据本身的时间
 *   2. 时间窗口的计算，看的是时间，如果是处理时间语义，直接看系统时间，如果是事件时间语义， 看水位线
 *      计算窗口的计算，看数据的条数
 *   3. 窗口是动态创建的，当有数据需要落到该窗口的时候，会创建该窗口
 *   4. 窗口计算输出结果 和 窗口关闭 是两个行为，默认窗口计算输出结果，直接关闭窗口。
 *   5. 使用窗口的时候，重点关注两件事:
 *       1) 窗口分配器， 决定了使用哪种类型的窗口
 *       2) 窗口函数， 决定了窗口中的数据如何进行计算
 * 窗口分配:
 *    如果是处理时间语义的窗口， 按照数据到达的时候，获取一次当前的处理时间(系统时间) ， 通过处理时间计算窗口。
 *    如果是事件时间语义的窗口， 按照数据中的时间，通过事件时间计算窗口
 *
 * 按键分区窗口
 */
public class Flink04_NonkeyedWindow {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.getConfig().setAutoWatermarkInterval(1000);

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
        //非按键分区窗口
        // 计数窗口 用的都是countWindowALL 里面两个参数，一个窗口大小，一个滑动大小
            // 不传滑动大小则是计数滚动，否则则是计数滑动
        // 计数滚动窗口                      // 用Tuple1 会有泛型擦除,所以用returns
        //ds.map( event -> Tuple1.of(1L)).returns(Types.TUPLE( Types.LONG))
        //  .countWindowAll(3L)
        //  .sum(0)
        //  .print();

        //  计数滑动窗口
        //ds.map( event -> Tuple1.of(1L)).returns(Types.TUPLE(Types.LONG))
        //        .countWindowAll(3L, 2L)  // 传两个参数则是滑动窗口
        //        .sum(0)
        //        .print();

        // 时间窗口
       ds.map( event -> Tuple1.of(1L)).returns(Types.TUPLE(Types.LONG))
               .windowAll(
                       // 时间滚动窗口
                       //处理时间
                       //TumblingProcessingTimeWindows.of(Time.seconds(5))

                       //如果窗口大小为1天， 需要设置offset为-8， 得到 [0 - 24)的窗口
                       //TumblingProcessingTimeWindows.of(Time.days(1) , Time.hours(-8))

                       //事件时间
                       TumblingEventTimeWindows.of(Time.seconds(5))

                       // 时间滑动窗口
                       // 处理时间
                       //SlidingProcessingTimeWindows.of(Time.seconds(10) , Time.seconds(5))

                       // 事件时间
                       //SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))

                       // 时间会话窗口
                       // 处理时间
                       //ProcessingTimeSessionWindows.withGap(Time.seconds(5))

                       // 事件时间
                       //EventTimeSessionWindows.withGap(Time.seconds(5))

                       //全局窗口
                       //GlobalWindows.create()
               )
               .sum(0)
               .print();
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

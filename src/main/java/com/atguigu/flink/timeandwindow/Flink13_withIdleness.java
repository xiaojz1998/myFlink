package com.atguigu.flink.timeandwindow;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.UrlViewCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Flink13_withIdleness {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port" , 5678);



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.getConfig().setAutoWatermarkInterval(1000L);

        SingleOutputStreamOperator<Event> ds1 = env.socketTextStream("hadoop102", 8888).name("sk8888")
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                        }
                ).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        (element, ts) -> element.getTs()
                                ).withIdleness(Duration.ofSeconds(10))
                );
        ds1.print("DS1");

        SingleOutputStreamOperator<Event> ds2 = env.socketTextStream("hadoop102", 9999).name("sk9999")
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                        }
                ).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        (element, ts) -> element.getTs()
                                ).withIdleness(Duration.ofSeconds(10))
                );

        ds2.print("DS2");

        DataStream<Event> unionDs = ds1.union(ds2);

        //统计每10内每个url的点击次数
        SingleOutputStreamOperator<UrlViewCount> windowDS = unionDs.keyBy(
                        Event::getUrl
                ).window(
                        TumblingEventTimeWindows.of(Time.seconds(10))
                )
                .aggregate(
                        new AggregateFunction<Event, UrlViewCount, UrlViewCount>() {
                            @Override
                            public UrlViewCount createAccumulator() {
                                return new UrlViewCount(null, null, null, 0L);
                            }

                            @Override
                            public UrlViewCount add(Event value, UrlViewCount accumulator) {
                                //accumulator.setUrl( value.getUrl());
                                accumulator.setCount(accumulator.getCount() + 1);
                                return accumulator;
                            }

                            @Override
                            public UrlViewCount getResult(UrlViewCount accumulator) {
                                return accumulator;
                            }

                            @Override
                            public UrlViewCount merge(UrlViewCount a, UrlViewCount b) {
                                return null;
                            }
                        },
                        new ProcessWindowFunction<UrlViewCount, UrlViewCount, String, TimeWindow>() {
                            @Override
                            public void process(String key, ProcessWindowFunction<UrlViewCount, UrlViewCount, String, TimeWindow>.Context context, Iterable<UrlViewCount> elements, Collector<UrlViewCount> out) throws Exception {
                                // 获取增量聚合的结果
                                UrlViewCount urlViewCount = elements.iterator().next();
                                //设置url
                                urlViewCount.setUrl(key);
                                //设置窗口信息
                                urlViewCount.setWindowStart(context.window().getStart());
                                urlViewCount.setWindowEnd(context.window().getEnd());

                                //输出
                                out.collect(urlViewCount);
                            }
                        }
                );

        windowDS.print("WINDOW");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}


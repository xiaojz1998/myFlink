package com.atguigu.flink.state;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.UrlViewCount;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * 网站中一个非常经典的例子，就是实时统计一段时间内的热门url。
 * 例如，需要统计最近10秒钟内最热门的两个url链接，并且每5秒钟更新一次。
 * 我们知道，这可以用一个滑动窗口来实现，而“热门度”一般可以直接用访问量来表示。
 * 于是就需要开滑动窗口收集url的访问数据，按照不同的url进行统计，而后汇总排序并最终输出前两名。
 * 这其实就是著名的“Top N”问题。
 *
 * 使用全窗口函数实现， 将窗口中的数据收集起来， 等到窗口触发计算时， 对收集的数据进行统计(统计每个url的点击次数) , 再对窗口统计的结果进行排序，输出前N名
 */
public class Flink12_TopN_ProcessAllWindowFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(2000L);
        SingleOutputStreamOperator<Event> ds = env.fromSource(SourceUtil.getSource(), WatermarkStrategy.noWatermarks(), "source")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        (e, t) -> e.getTs()
                                )
                );
        ds.print("INPUT");
        //统计最近10秒钟内最热门的两个url链接，并且每5秒钟更新一次
        // 窗口: 事件时间语义滑动窗口
        ds.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(
                        new ProcessAllWindowFunction<Event, String, TimeWindow>() {
                            @Override
                            public void process(ProcessAllWindowFunction<Event, String, TimeWindow>.Context context, Iterable<Event> iterable, Collector<String> collector) throws Exception {
                                // 窗口信息
                                long start = context.window().getStart();
                                long end = context.window().getEnd();
                                // 统计每个url的点击次数
                                HashMap<String,Long> urlViewCountMap = new HashMap<>();
                                for (Event event : iterable) {
                                    // 从map中取出当前event的url对应的点击次数，如果没有就默认为0
                                    Long count = urlViewCountMap.getOrDefault(event.getUrl(), 0L);
                                    // 更新点击次数
                                    urlViewCountMap.put(event.getUrl(), count + 1);
                                }

                                // 转换成list
                                ArrayList<UrlViewCount> urlViewCounts = new ArrayList<>();
                                urlViewCountMap.forEach((k, v)-> urlViewCounts.add(UrlViewCount.builder().windowStart(start).windowEnd(end).url(k).count(v).build()));

                                // 排序
                                urlViewCounts.sort((a,b)-> Long.compare(b.getCount(), a.getCount()));
                                //输出前N名
                                StringBuilder topNBuilder = new StringBuilder("==================Top2===============\n");
                                for (int i = 0; i < Math.min(2, urlViewCounts.size()); i++) {
                                    topNBuilder.append("Top." + (i + 1) + " " + urlViewCounts.get(i) + "\n");
                                }
                                topNBuilder.append("=====================================\n");
                                // 输出
                                collector.collect(topNBuilder.toString());
                            }
                        }
                ).print();


        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}

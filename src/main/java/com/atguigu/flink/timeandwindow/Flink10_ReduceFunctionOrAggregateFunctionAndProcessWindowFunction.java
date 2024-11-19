package com.atguigu.flink.timeandwindow;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.UrlViewCount;
import com.atguigu.flink.pojo.WordCount;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 增量聚合函数 和 全窗口函数结合使用:
 *    使用增量聚合函数完成聚合过程， 再使用全窗口函数获取窗口信息
 */
public class Flink10_ReduceFunctionOrAggregateFunctionAndProcessWindowFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1).getConfig().setAutoWatermarkInterval(1000);
        SingleOutputStreamOperator<Event> ds = env.fromSource(SourceUtil.getSource(), WatermarkStrategy.noWatermarks(), "dataGenSource")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        (event, ts) -> event.getTs()
                                )
                );
        ds.print("INPUT");
        // 统计8秒内每url点击次数, 输出包含窗口信息：
        // 窗口: 按键分区事件时间滚动窗口
        // 在aggregate中传两个对象，一个是增量方法，一个是全量方法
        ds.map(event -> new WordCount(event.getUrl(), 1L))
                .keyBy(WordCount::getWord)
                .window(TumblingEventTimeWindows.of(Time.seconds(8)))
                .aggregate(
                        // 增量聚合函数
                        new AggregateFunction<WordCount, UrlViewCount, UrlViewCount>() {
                            @Override
                            public UrlViewCount createAccumulator() {
                                return new UrlViewCount(null, null, null, 0L);
                            }

                            @Override
                            public UrlViewCount add(WordCount wordCount, UrlViewCount urlViewCount) {
                                UrlViewCount result = urlViewCount.builder()
                                        .url(wordCount.getWord())
                                        .count(urlViewCount.getCount() + 1)
                                        .build();
                                return result;
                            }

                            @Override
                            public UrlViewCount getResult(UrlViewCount urlViewCount) {
                                return urlViewCount;
                            }

                            @Override
                            public UrlViewCount merge(UrlViewCount urlViewCount, UrlViewCount acc1) {
                                return null;
                            }
                        },
                        // 全窗口函数 ， 主要就是获取窗口信息
                        new ProcessWindowFunction<UrlViewCount, UrlViewCount, String, TimeWindow>() {
                            //@param iterable  先使用了增量聚合， 再使用全窗口函数， 会将增量聚合的结果传过来，实际上只有一条数据。
                            @Override
                            public void process(String key, ProcessWindowFunction<UrlViewCount, UrlViewCount, String, TimeWindow>.Context context, Iterable<UrlViewCount> iterable, Collector<UrlViewCount> collector) throws Exception {
                                // 获得上下文的开始和结束时间
                                long start = context.window().getStart();
                                long end = context.window().getEnd();
                                // 获取归约后的结果
                                UrlViewCount result = iterable.iterator().next();
                                // 给result赋值
                                result.setWindowStart(start);
                                result.setWindowEnd(end);
                                collector.collect(result);
                            }
                        }
                ).print("WINDOW");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

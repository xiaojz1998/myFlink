package com.atguigu.flink.timeandwindow;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.UrlViewCount;
import com.atguigu.flink.pojo.WordCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * Flink对迟到数据的处理:
 *   1. 延迟水位线的推进 ， 生成水位线的时候人为的减去一个延迟时间(经过监控，确定绝大多数的数据的延迟时间 ， 保证绝大多数的数据能正确参与窗口计算)
 *   2. 延迟窗口的关闭 ， 窗口触发计算输出结果， 窗口不关闭，再等待一段时间（保证少量的迟到更久的数据能正确的参与窗口计算）
 *			以后每来一条迟到数据，就触发一次这条数据所在窗口计算(增量计算)。
 *          直到wartermark 超过了窗口结束时间+推迟时间，此时窗口会真正关闭。
 *   3. 使用侧输出流接收极端数据（保证个别的迟到特别久的数据不丢失，再想办法对该部分数据进行额外的处理）
 */
public class Flink11_LateData {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(500L);
        SingleOutputStreamOperator<Event> ds = env.socketTextStream("hadoop102", 8888)
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                        }

                ).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2)) // 1. 延迟水位线的推进 ，
                                .withTimestampAssigner(
                                        (event, ts) -> event.getTs()
                                )
                );
        ds.print("INPUT");

        // 3. 侧输出流标签
        OutputTag<Event> lateOutputTag =new OutputTag<>("lateOutputTag", Types.POJO(Event.class));

        // 需求: 统计每8秒内每个url的点击次数
        SingleOutputStreamOperator<UrlViewCount> ds1 = ds.keyBy(event -> event.getUrl())
                .window(TumblingEventTimeWindows.of(Time.seconds(8)))
                .allowedLateness(Time.seconds(4)) // 2. 延迟窗口的关闭
                .sideOutputLateData(lateOutputTag)// 3. 使用侧输出流接收极端数据
                .aggregate(
                        // 增量聚合函数
                        new AggregateFunction<Event, UrlViewCount, UrlViewCount>() {
                            @Override
                            public UrlViewCount createAccumulator() {
                                return new UrlViewCount(null, null, null, 0L);
                            }

                            @Override
                            public UrlViewCount add(Event event, UrlViewCount urlViewCount) {
                                UrlViewCount result = urlViewCount.builder()
                                        .url(event.getUrl())
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
                );

        //捕获侧输出流
        SideOutputDataStream<Event> sideOutput = ds1.getSideOutput(lateOutputTag);
        ds1.print("WINDOW");

        sideOutput.print("LATE");
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

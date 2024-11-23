package com.atguigu.flink.state;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.UrlViewCount;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * 网站中一个非常经典的例子，就是实时统计一段时间内的热门url。
 * 例如，需要统计最近10秒钟内最热门的两个url链接，并且每5秒钟更新一次。
 * 我们知道，这可以用一个滑动窗口来实现，而“热门度”一般可以直接用访问量来表示。
 * 于是就需要开滑动窗口收集url的访问数据，按照不同的url进行统计，而后汇总排序并最终输出前两名。
 * 这其实就是著名的“Top N”问题。
 *
 * 使用增量聚合 + 全窗口函数 + 状态 + 定时器
 *
 * 1. 使用增量聚合 + 全窗口函数完成UrlViewCount
 * 2. 按照窗口的结束时间进行 keyBy,将同一个时间范围窗口输出的数据收集到一起
 * 3. 下游算子将输入的数据存入到状态中，并注册窗口结束时间的定时器
 * 4. 定时器触发，表示来自于同一个窗口的数据都收集齐了， 对状态中的数据进行排序，输出topN
 *
 */
public class Flink13_TopN_AggregateFunctionAndProcessWindowFunctionAndStateAndTimer {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(500l);

        SingleOutputStreamOperator<Event> ds = env.fromSource(SourceUtil.getSource(), WatermarkStrategy.noWatermarks(), "MySource")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        (e, t) -> e.getTs()
                                )
                );
        ds.print("INPUT");
        //统计最近10秒钟内最热门的两个url链接，并且每5秒钟更新一次
        // 窗口: 事件时间语义滑动窗口
        //1. 使用增量聚合 + 全窗口函数完成UrlViewCount
        SingleOutputStreamOperator<UrlViewCount> aggregateDs = ds.keyBy(Event::getUrl)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(
                        new AggregateFunction<Event, UrlViewCount, UrlViewCount>() {
                            @Override
                            public UrlViewCount createAccumulator() {
                                return new UrlViewCount(null, null, null, 0L);
                            }

                            @Override
                            public UrlViewCount add(Event event, UrlViewCount urlViewCount) {
                                urlViewCount.setCount(urlViewCount.getCount() + 1);
                                return urlViewCount;
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
                        new ProcessWindowFunction<UrlViewCount, UrlViewCount, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<UrlViewCount, UrlViewCount, String, TimeWindow>.Context context, Iterable<UrlViewCount> iterable, Collector<UrlViewCount> collector) throws Exception {
                                // 获取窗口信息
                                long start = context.window().getStart();
                                long end = context.window().getEnd();
                                // 设置key
                                UrlViewCount next = iterable.iterator().next();
                                next.setUrl(s);
                                // 设置窗口信息
                                next.setWindowStart(start);
                                next.setWindowEnd(end);
                                // 输出
                                collector.collect(next);

                            }
                        }
                );
        aggregateDs.print("aggregate");

        //2. 按照窗口的结束时间进行 keyBy,将同一个时间范围窗口输出的数据收集到一起
        // 这里用定时器的原理是： 上游窗口计算完，数据一定会在触发窗口关闭的时间戳前发出，后面的时间戳大于窗口end
        SingleOutputStreamOperator<String> processDs = aggregateDs.keyBy(UrlViewCount::getWindowEnd)
                .process(
                        new KeyedProcessFunction<Long, UrlViewCount, String>() {
                            // 声明状态
                            // 用于存储urlViewCount
                            private ListState<UrlViewCount> listState;
                            // 用于条件判断
                            private ValueState<Boolean> valueState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                // 初始化状态
                                RuntimeContext runtimeContext = getRuntimeContext();
                                ListStateDescriptor<UrlViewCount> urlViewCountListStateDesc = new ListStateDescriptor<>("urlViewCountListState", Types.POJO(UrlViewCount.class));
                                listState = runtimeContext.getListState(urlViewCountListStateDesc);

                                ValueStateDescriptor<Boolean> timerStateDesc = new ValueStateDescriptor<>("timerStateDesc", Types.BOOLEAN);
                                valueState = runtimeContext.getState(timerStateDesc);
                            }

                            @Override
                            public void processElement(UrlViewCount urlViewCount, KeyedProcessFunction<Long, UrlViewCount, String>.Context context, Collector<String> collector) throws Exception {
                                //3. 下游算子将输入的数据存入到状态中，并注册窗口结束时间的定时器
                                listState.add(urlViewCount);
                                if (valueState.value() == null) {
                                    context.timerService().registerEventTimeTimer(context.getCurrentKey());
                                    System.out.println("设置定时器："+context.getCurrentKey() );
                                    valueState.update(true);
                                }
                            }

                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                                System.out.println("onTimer ==> 当前的水位线: " + ctx.timerService().currentWatermark());
                                // 定时器触发，表示来自于同一个窗口的数据都收集齐了， 对状态中的数据进行排序，输出topN
                                // 从状态中取出所有数据
                                Iterable<UrlViewCount> urlViewCounts = listState.get();
                                // 存到可排序的list集合中 并排序
                                List<UrlViewCount> list = new ArrayList<>();
                                urlViewCounts.forEach(list::add);
                                list.sort((a, b) -> Long.compare(b.getCount(), a.getCount()));
                                // 取topN
                                StringBuilder stringBuilder = new StringBuilder("==================Top2===============\n");
                                for (int i = 0; i < 2 && i < list.size(); i++) {
                                    stringBuilder.append("Top." + (i + 1) + " " + list.get(i) + "\n");
                                }
                                stringBuilder.append("===================================================\n");
                                // 输出
                                out.collect(stringBuilder.toString());
                                // 清除
                                listState.clear();
                            }
                        }
                );
        processDs.print("Result");


        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}

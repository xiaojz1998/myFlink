package com.atguigu.flink.timeandwindow;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.WordCount;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashMap;

/**
 * 全窗口函数:
 *      ProcessWindowFunction , 将进入到窗口的数据收集起来，等到窗口触发计算时， 将所收集的数据一次性进行计算，输出结果。 支持获取窗口信息
 *         四个泛型:
 *          <IN> – The type of the input value.    输入类型
 *          <OUT> – The type of the output value.  输出类型
 *          <KEY> – The type of the key.  Key的类型,即keyby操作的key的类型，不用写，因为上面做的keyBy他知道什么类型
 *          <W> – The type of Window that this window function can be applied on.  窗口类型
 *                时间窗口: TimeWindow
 *                计数窗口: GlobalWindow
 */
public class Flink09_ProcessWindowFunction {
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
        // 统计10秒内每用户点击次数, 输出包含窗口信息：
        // 窗口: 按键分区事件时间滚动窗口
        ds.map(event -> new WordCount(event.getUser(), 1L))
                .keyBy(WordCount::getWord)
                .window(TumblingEventTimeWindows.of(Time.seconds(8)))
                .process(
                        // 泛型介绍看上面
                        new ProcessWindowFunction<WordCount, String, String, TimeWindow>() {
                            // keyBy的key 上下文对象 所有数据对象的迭代器 输出
                            @Override
                            public void process(String key, ProcessWindowFunction<WordCount, String, String, TimeWindow>.Context context, Iterable<WordCount> iterable, Collector<String> collector) throws Exception {
                                HashMap<String,Long> s = new HashMap<>();
                                for (WordCount i : iterable) {
                                    if(s.get(i.getWord())!=null) s.put(i.getWord(),s.get(i.getWord())+1);
                                    else s.put(i.getWord(),1L);
                                }
                                // 获取窗口信息（）
                                long start = context.window().getStart();
                                long end = context.window().getEnd();
                                // 输出
                                s.forEach((k,v)->collector.collect("窗口：" + start + "~" + end + "，用户：" + k + "，点击次数：" + v));
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

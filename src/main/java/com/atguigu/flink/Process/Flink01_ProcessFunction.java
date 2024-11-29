package com.atguigu.flink.Process;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 处理算子 ： process
 * 处理函数:  ProcessFunction
 * 处理函数的功能:
 *    1. 处理数据的功能  processElement()
 *    2. 拥有富函数的功能，  生命周期方法  和  按键分区状态编程
 *    3. 定时器编程
 *          TimerService:
 *               long currentProcessingTime();
 *               void registerProcessingTimeTimer(long time);
 *               void deleteProcessingTimeTimer(long time);
 *
 *               long currentWatermark();
 *               void registerEventTimeTimer(long time);
 *               void deleteEventTimeTimer(long time);
 *
 *         onTimer() :  当定时器触发后，会调用该方法
 *   4. 使用侧输出流
 *         public abstract <X> void output(OutputTag<X> outputTag, X value);
 * 常用的处理函数:
 *          函数      =>  上一级调用他的流 => process()
 *    1) ProcessFunction   => DataStream  => process()
 *    2) KeyedProcessFunction  => KeyedStream  => process()
 *    3) ProcessWindowFunction => WindowedStream => process()
 *    4) ProcessAllWindowFunction  => AllWindowedStream  => process()
 *    5) CoProcessFunction   => ConnectedStreams  => process()
 *    6) ProcessJoinFunction  => IntervalJoined => process()
 *    7) BroadcastProcessFunction => BroadcastConnectedStream  => process()
 *    8) KeyedBroadcastProcessFunction => BroadcastConnectedStream  => process()
 *
 *  使用定时器的注意事项:
 *     Setting timers is only supported on a keyed streams.
 *     要定时， 先keyBy
 *     同一个key在同一时间注册多个定时器，未来只会触发一次.
 *     不同的key在同一时间注册多个定时器， 未来每个key的定时器都会触发一次。
 *
 */
// 以下为定时器相关的代码
    // TODO 事件时间
public class Flink01_ProcessFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 需求:
        // 处理时间: 按照数据到达的时间为基准， 5秒后执行指定的逻辑。
        // 事件时间: 按照数据中的时间为基准， 5秒后执行指定的逻辑。
        SingleOutputStreamOperator<Event> ds = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] split = line.split(",");
                    return new Event(split[0], split[1], Long.valueOf(split[2]));
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        (e, t) -> e.getTs()
                                )
                );
        SingleOutputStreamOperator<String> processDs = ds.keyBy(Event::getUser)
                .process(
                        new KeyedProcessFunction<String, Event, String>() {
                            @Override
                            public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
                                // 获取TimerService
                                TimerService timerService = context.timerService();
                                // 处理时间定时器
                                // 获取当前处理时间
                                //long currentProcessingTime = timerService.currentProcessingTime();
                                //System.out.println("processElement ==> 当前的处理时间为: " + currentProcessingTime);
                                // 注册5s后的定时器
                                //long timerTime = currentProcessingTime + 5000L;
                                //timerService.registerProcessingTimeTimer(timerTime);
                                //System.out.println("processElement ==> 注册了处理时间定时器: " + timerTime);


                                //事件时间定时器
                                //获取当前数据的时间
                                Long ts = event.getTs();
                                System.out.println("processElement ==> 当前数据的时间为: " + ts);
                                //注册5秒后的事件时间定时器
                                Long timerTime = ts + 5000L;
                                timerService.registerEventTimeTimer(timerTime);
                                System.out.println("processElement ==> 注册了事件时间定时器: " + timerTime + " , 当前key: " + context.getCurrentKey());

                                // 对输入数据进行处理
                                // 输出
                                collector.collect(JSON.toJSONString(event));

                            }

                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                                //System.out.println("onTimer ==> 当前触发的处理时间定时器为: " + timestamp);
                                //TimerService timerService = ctx.timerService();
                                //long currentProcessingTime = timerService.currentProcessingTime();
                                //System.out.println("onTimer ==> 当前的处理时间为: " + currentProcessingTime);

                                //当前key
                                String currentKey = ctx.getCurrentKey();

                                System.out.println("onTimer ==> 当前触发的事件时间定时器为: " + timestamp + " , 当前key: " + currentKey  );
                                TimerService timerService = ctx.timerService();
                                long currentWatermark = timerService.currentWatermark();
                                System.out.println("onTimer ==> 当前的事件时间(水位线)为: " + currentWatermark);
                            }
                        }
                );

        processDs.print("PROCESS");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }
}

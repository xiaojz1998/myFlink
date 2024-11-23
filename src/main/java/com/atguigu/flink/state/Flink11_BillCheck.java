package com.atguigu.flink.state;

import com.atguigu.flink.pojo.AppEvent;
import com.atguigu.flink.pojo.ThirdPartyEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 对账案例: 我们可以实现一个实时对账的需求，也就是app的支付操作和第三方的支付操作的一个双流Join。
 *         App的支付事件和第三方的支付事件将会互相等待5秒钟，如果等不来对应的支付事件，那么就输出报警信息。
 */
public class Flink11_BillCheck {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(2000L);
        // AppEvent: 1001,order-1,pay,1000
        SingleOutputStreamOperator<AppEvent> appDs = env.socketTextStream("hadoop102", 8888)
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new AppEvent(fields[0].trim(), fields[1].trim(), fields[2].trim(), Long.valueOf(fields[3].trim()));
                        }
                ).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<AppEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        (element, ts) -> element.getTs()
                                )
                );

        // ThirdPartyEvent: 1001,order-1,pay,wechat,2000
        SingleOutputStreamOperator<ThirdPartyEvent> thirdPartyDs = env.socketTextStream("hadoop102", 9999)
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new ThirdPartyEvent(fields[0].trim(), fields[1].trim(), fields[2].trim(), fields[3].trim(), Long.valueOf(fields[4].trim()));
                        }
                ).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ThirdPartyEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        (element, ts) -> element.getTs()
                                )
                );
        thirdPartyDs.print("TPD");
        SingleOutputStreamOperator<AppEvent> filterAppDs = appDs.filter(data -> "pay".equals(data.getEventType()));
        filterAppDs.print("APP");

        // 用keyBy+connect 将两条流合并
        SingleOutputStreamOperator<String> processDs = filterAppDs.keyBy(AppEvent::getOrderId)
                .connect(thirdPartyDs.keyBy(ThirdPartyEvent::getOrderId))
                .process(
                        new KeyedCoProcessFunction<String, AppEvent, ThirdPartyEvent, String>() {
                            // 1. 声明状态
                            //      用于存储orderid相同的两个订单
                            private ValueState<AppEvent> appEventState;
                            private ValueState<ThirdPartyEvent> thirdPartyEventState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                // 在open中初始化两个状态
                                RuntimeContext runtimeContext = getRuntimeContext();
                                ValueStateDescriptor<AppEvent> appEventStateDes = new ValueStateDescriptor<>("appEventState", Types.POJO(AppEvent.class));
                                appEventState = runtimeContext.getState(appEventStateDes);
                                ValueStateDescriptor<ThirdPartyEvent> thirdPartyEventStateDes = new ValueStateDescriptor<>("thirdPartyEventState", Types.POJO(ThirdPartyEvent.class));
                                thirdPartyEventState = runtimeContext.getState(thirdPartyEventStateDes);
                            }

                            /**
                             * 处理appDs
                             * 1. AppEvent到了 ， 到状态中找 ThirdPartyEvent
                             * 2. 如果找到， 说明ThirdPartyEvent先到， 从状态中取出ThirdPartyEvent, 对账成功， 删除ThirdPartyEvent定时器 , 清除状态
                             * 3. 如果找不到， 说明AppEvent先到， 将AppEvent放到状态中， 并注册5秒后的定时器
                             * 4. 如果定时器触发了， 对账失败。
                             */

                            @Override
                            public void processElement1(AppEvent appEvent, KeyedCoProcessFunction<String, AppEvent, ThirdPartyEvent, String>.Context context, Collector<String> collector) throws Exception {
                                ThirdPartyEvent thirdPartyValue = thirdPartyEventState.value();
                                if (thirdPartyValue != null) {
                                    collector.collect(appEvent.getOrderId() + " 对账成功 ,  ThirdPartyEvent先到, AppEvent后到");
                                    context.timerService().deleteEventTimeTimer(thirdPartyValue.getTs() + 5000L);
                                    thirdPartyEventState.clear();
                                } else {
                                    appEventState.update(appEvent);
                                    context.timerService().registerEventTimeTimer(appEvent.getTs() + 5000L);
                                }
                            }

                            /**
                             * 处理thirdPartyDs
                             *
                             * 1. ThirdPartyEvent 到了 ， 到状态中找 AppEvent
                             * 2. 如果找到， 说明 AppEvent 先到， 从状态中取出 AppEvent, 对账成功， 删除 AppEvent 定时器, 清除状态
                             * 3. 如果找不到， 说明 ThirdPartyEvent 先到， 将 ThirdPartyEvent 放到状态中， 并注册5秒后的定时器
                             * 4. 如果定时器触发了， 对账失败。
                             */
                            @Override
                            public void processElement2(ThirdPartyEvent thirdPartyEvent, KeyedCoProcessFunction<String, AppEvent, ThirdPartyEvent, String>.Context context, Collector<String> collector) throws Exception {
                                AppEvent appEventValue = appEventState.value();
                                if (appEventValue != null) {
                                    collector.collect(thirdPartyEvent.getOrderId() + " 对账成功 ,  AppEvent先到, ThirdPartyEvent后到");
                                    context.timerService().deleteEventTimeTimer(appEventValue.getTs() + 5000L);
                                    appEventState.clear();
                                } else {
                                    thirdPartyEventState.update(thirdPartyEvent);
                                    context.timerService().registerEventTimeTimer(thirdPartyEvent.getTs() + 5000L);
                                }
                            }

                            @Override
                            public void onTimer(long timestamp, KeyedCoProcessFunction<String, AppEvent, ThirdPartyEvent, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                                // 如果定时器触发了，对账失败
                                if (appEventState.value() != null) {
                                    out.collect(ctx.getCurrentKey() + " 对账失败, AppEvent到了, ThirdPartyEvent未到!!!!");
                                } else if (thirdPartyEventState.value() != null) {
                                    out.collect(ctx.getCurrentKey() + " 对账失败, ThirdPartyEvent到了, AppEvent未到!!!!");
                                }
                                // 清除状态
                                appEventState.clear();
                                thirdPartyEventState.clear();
                            }
                        }
                );

        processDs.print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

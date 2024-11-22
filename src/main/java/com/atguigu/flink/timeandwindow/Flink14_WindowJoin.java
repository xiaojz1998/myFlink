package com.atguigu.flink.timeandwindow;

import com.atguigu.flink.pojo.OrderDetailEvent;
import com.atguigu.flink.pojo.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * WindowJoin: 来自于两条流的相同key(join条件)的数据， 如果能进入到同一个时间范围的窗口中，即可join成功。
 *                  需要注意两条相同key的数据，相差几毫秒，但是正好卡到窗口的结束边界上，
 *                  一个进入上个窗口， 一个进入下个窗口， join失败。
 */
public class Flink14_WindowJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // orderEvent数据内容:  order-1,1000
        SingleOutputStreamOperator<OrderEvent> orderDs = env.socketTextStream("hadoop102", 8888)
                .map(
                        line -> {
                            String[] sl = line.split(",");
                            return new OrderEvent(sl[0].trim(), Long.valueOf(sl[1].trim()));
                        }
                ).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        (element, ts) -> element.getTs()
                                )
                );
        orderDs.print("ORDER");
        // OrderDetailEvent数据内容 :  detail-1,order-1,2000
        SingleOutputStreamOperator<OrderDetailEvent> orderDetailDs = env.socketTextStream("hadoop102", 9999)
                .map(
                        line -> {
                            String[] sl = line.split(",");
                            return new OrderDetailEvent(sl[0].trim(), sl[1].trim(), Long.valueOf(sl[2].trim()));
                        }
                ).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderDetailEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        (element, ts) -> element.getTs()
                                )
                );
        orderDetailDs.print("DETAIL");

        // 以下用windowJoin 格式是固定的
        orderDs.join(orderDetailDs)
                .where(OrderEvent::getId)   // 指定orderDs中的key
                .equalTo(OrderDetailEvent::getOrderId)  // 指定orderDetailDs中的key
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(
                        new JoinFunction<OrderEvent, OrderDetailEvent, String>() {
                            //处理Join成功的数据
                            // 第一个参数是orderDs的数据，第二个参数是orderDetailDs的数据
                            @Override
                            public String join(OrderEvent orderEvent, OrderDetailEvent orderDetailEvent) throws Exception {
                                return orderEvent + "==" + orderDetailEvent;
                            }
                        }
                        //new FlatJoinFunction<OrderEvent, OrderDetailEvent, String>() {
                        //    @Override
                        //    public void join(OrderEvent orderEvent, OrderDetailEvent orderDetailEvent, Collector<String> collector) throws Exception {
                        //        collector.collect(orderEvent + "==" + orderDetailEvent);
                        //    }
                        //}
                ).print("JOIN");



        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}

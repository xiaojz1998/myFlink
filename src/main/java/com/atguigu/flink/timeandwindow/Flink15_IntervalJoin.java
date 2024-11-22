package com.atguigu.flink.timeandwindow;

import com.atguigu.flink.pojo.OrderDetailEvent;
import com.atguigu.flink.pojo.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 *
 * IntervalJoin: 来自于两条流的相同key(join条件)的数据，  以一条流中数据的时间为基准，设置一个上界和下界， 形成一个时间范围，
 *               另外一条流对应的数据只要能落到该时间范围内，就可以Join成功
 */
public class Flink15_IntervalJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // orderEvent:  order-1,1000
        SingleOutputStreamOperator<OrderEvent> orderDS = env.socketTextStream("hadoop102", 8888)
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new OrderEvent(fields[0].trim(), Long.valueOf(fields[1].trim()));
                        }
                ).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        (event, ts) -> event.getTs()
                                )
                );

        orderDS.print("ORDER");
        // OrderDetailEvent :  detail-1,order-1,2000
        SingleOutputStreamOperator<OrderDetailEvent> orderDetailDS = env.socketTextStream("hadoop102", 9999)
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new OrderDetailEvent(fields[0].trim(),  fields[1].trim() , Long.valueOf(fields[2].trim()));
                        }
                ).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderDetailEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        (event, ts) -> event.getTs()
                                )
                );
        orderDetailDS.print("DETAIL");

        // IntervalJoin
        orderDS.keyBy(OrderEvent::getId)
                .intervalJoin(orderDetailDS.keyBy(OrderDetailEvent::getOrderId))
                .between(Time.seconds(-2), Time.seconds(2))
                //.lowerBoundExclusive() // 排除下边界的值
                //.upperBoundExclusive() // 排除上边界的值
                .process(
                        new ProcessJoinFunction<OrderEvent, OrderDetailEvent, String>() {
                            @Override
                            public void processElement(OrderEvent orderEvent, OrderDetailEvent orderDetailEvent, ProcessJoinFunction<OrderEvent, OrderDetailEvent, String>.Context context, Collector<String> collector) throws Exception {
                                collector.collect(orderEvent + "==" + orderDetailEvent);
                            }
                        }
                ).print("JOIN");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

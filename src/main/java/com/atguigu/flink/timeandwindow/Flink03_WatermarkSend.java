package com.atguigu.flink.timeandwindow;

import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 *  水位线传递: 上游给下游采用广播的方式发送水位线， 下游收到来自多个上游的水位线，以最小的为主。
 */
public class Flink03_WatermarkSend {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",5678);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.disableOperatorChaining();
        env.getConfig().setAutoWatermarkInterval(500);

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
                                )
                );

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
                                )
                );

        ds1.print("input");


        ds1.union(ds2)
                .map(e->e)
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}

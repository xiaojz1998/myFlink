package com.atguigu.flink.timeandwindow;

import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 *  Flink 内置的水位线生成器
 */
public class Flink02_FlinkEmbeddedWatermark {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",5678);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.getConfig().setAutoWatermarkInterval(500);
        env.disableOperatorChaining();
        SingleOutputStreamOperator<Event> ds = env.socketTextStream("hadoop102", 8888)
                .map((s) -> {
                    String[] split = s.split(",");
                    return new Event(split[0], split[1], Long.parseLong(split[2]));
                });

        // 生成水位线
        SingleOutputStreamOperator<Event> timestampsAndWatermarks = ds.assignTimestampsAndWatermarks(
                // 有序流
                //WatermarkStrategy.<Event>forMonotonousTimestamps()
                //        .withTimestampAssigner((e, t) -> e.getTs())
                // 无序流
                //WatermarkStrategy.<Event>forBoundedOutOfOrderness()
                //.withTimestampAssigner((e, t) -> e.getTs())
                // 有序+乱序流
                WatermarkStrategy
                        //.<Event>forBoundedOutOfOrderness(Duration.ZERO) 有序
                        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((e, t)-> e.getTs())
        );

        timestampsAndWatermarks.map(a ->a).print();

        try {
            env.execute();
        } catch (Exception e){
            throw new RuntimeException(e);
        }

    }
}

package com.atguigu.flink.datasteamapi.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.UUID;

/**
 * Flink从1.11开始提供了一个内置的DataGen 连接器，主要是用于生成一些随机数，用于在没有数据源的时候，进行流任务的测试以及性能测试等。
 * DataGen Connector
 *   DataGen Source
 */
public class Flink04_DataGenSource {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 数据生成器源
        DataGeneratorSource<String> dataGenSource = new DataGeneratorSource<>(
                // 第一个泛型是第几条数据，所以是long
                // 第二个泛型是返回值类型
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long aLong) throws Exception {
                        return UUID.randomUUID().toString() + " -> " + aLong;
                    }
                },
                //设置总记录数
                100,
                // 每秒生成记录数量
                RateLimiterStrategy.perSecond(1),
                // 指定返回类型
                Types.STRING
        );

        DataStreamSource<String> ds = env.fromSource(dataGenSource, WatermarkStrategy.noWatermarks(), "dataGenSource");

        ds.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

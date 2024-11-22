package com.atguigu.flink.datasteamapi.sink;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink04_JdbcSink {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

package com.atguigu.flink.datasteamapi.env;

import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 * 流式执行环境:  StreamExecutionEnvironment
 *   1. 创建执行环境
 *      1) StreamExecutionEnvironment.getExecutionEnvironment()
 *      2) StreamExecutionEnvironment.createLocalEnvironment();
 *      3) StreamExecutionEnvironment.createRemoteEnvironment("hadoop104", 35527, "xxx.jar");
 *   2. 执行模式
 *      流
 *      批
 *      自动
 *   3. 启动执行
 *      env..execute();
 *
 */
public class Flink01_StreamExecutionEnvironment {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println(env);
        LocalStreamEnvironment localenv = StreamExecutionEnvironment.createLocalEnvironment();
        System.out.println(localenv);

    }
}

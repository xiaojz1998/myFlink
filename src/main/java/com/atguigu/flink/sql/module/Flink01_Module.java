package com.atguigu.flink.sql.module;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.module.hive.HiveModule;

import java.util.Arrays;

/**
 * Flink支持通过Module扩展函数功能。
 */
public class Flink01_Module {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        // 引入HiveModule
        // 创建HiveModule
        HiveModule hiveModule = new HiveModule("3.1.3");
        // 加载HiveModule
        streamTableEnv.loadModule("hive" , hiveModule);

        // core , hive
        // 调整模块的顺序
        streamTableEnv.useModules("hive" , "core");

        String[] modules = streamTableEnv.listModules();
        System.out.println(Arrays.toString( modules ));

        streamTableEnv.sqlQuery( "select split('A,BB,CCC,DDDD' , ',')" ).execute().print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

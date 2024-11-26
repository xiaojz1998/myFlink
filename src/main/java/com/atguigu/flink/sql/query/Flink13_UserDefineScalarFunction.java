package com.atguigu.flink.sql.query;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import static org.apache.flink.table.api.Expressions.*;

import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 *
 * 用户自定义ScalarFunction :  一进一出
 */
public class Flink13_UserDefineScalarFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //流表环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        //流
        DataStreamSource<String> ds = env.socketTextStream("hadoop102", 8888);

        //流转表
        Table table = streamTableEnv.fromDataStream(ds);
        streamTableEnv.createTemporaryView("t1" , table );
        // (
        //  `f0` STRING
        //)
        table.printSchema();
        // 将输入的字符串数据转换成大写输出
        // 注册函数
        streamTableEnv.createTemporaryFunction("myupper", MyUpperScalarFunction.class);
        //使用函数
        // 1. tableAPI
        //table.select($("f0"),call("myupper",$("f0")).as("new_f0"))
        //        .execute()
        //        .print();
        // 2. sql
        streamTableEnv.executeSql(" select f0 , myupper(f0) new_f0 from t1").print();
    }
    public static class MyUpperScalarFunction extends ScalarFunction {
        public String eval(String str){
            return str.toUpperCase();
        }
    }
}

package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import static org.apache.flink.table.api.Expressions.*;

/**
 * 自定义AggregateFunction
 * 1. 继承 AggregateFunction
 * 2. 实现多个方法
 *      1) createAccumulator()
 *      2) accumulate()
 *      3) getValue()
 */
public class Flink14_UserDefineAggregateFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //流表环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        // 流
        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new WaterSensor(fields[0].trim(), Integer.valueOf(fields[1].trim()), Long.valueOf(fields[2].trim()));
                        }
                );

        //流转表
        Schema schema = Schema.newBuilder()
                .column("id" , "STRING")
                .column("vc" ,"INT")
                .column("ts" , "BIGINT")
                .columnByExpression("pt" , "PROCTIME()")
                .columnByExpression("et" , "TO_TIMESTAMP_LTZ(ts, 3)")
                .watermark("et" , "et - INTERVAL '2' SECOND ")
                .build();
        Table table = streamTableEnv.fromDataStream(ds, schema);
        streamTableEnv.createTemporaryView("t1" , table );
        // 计算每种传感器的平均水位记录
        //注册函数
        streamTableEnv.createTemporaryFunction("myAvgVc" , MyAvgVcAggregateFunction.class);
        // 使用函数
        // 1. api
        table.groupBy($("id"))
                .select($("id"), call("myAvgVc", $("vc")).as("avgVc"))
                .execute()
                .print();

        // 2. sql
        //streamTableEnv.sqlQuery(
        //        " select id , myAvgVc(vc) as avgVc from t1 group by id "
        //).execute().print();

    }
    public static class MyAvgVcAggregateFunction extends AggregateFunction<Double, Tuple2<Integer, Integer>>{
        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return Tuple2.of(0,0);
        }
        public void accumulate(Tuple2<Integer, Integer> accumulator, Integer vc) {
            accumulator.f0 += vc;
            accumulator.f1 += 1;
        }

        @Override
        public Double getValue(Tuple2<Integer, Integer> integerIntegerTuple2) {
            return (double)integerIntegerTuple2.f0 / integerIntegerTuple2.f1;
        }
    }
}

package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.*;

// 用户自定义TableAggregateFunction
public class Flink16_UserDefineTableAggregateFunction {
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
        // 求每种传感器水位记录的前两名
        // 注册自定义方法
        streamTableEnv.createTemporaryFunction("MYTOP2VC" , MyTop2VcTableAggregateFunction.class);
        //使用函数
        // API
        table.groupBy($("id"))
                .flatAggregate(call("MYTOP2VC", $("vc")))
                .select($("id"), $("rank"),$("rk_vc"))
                .execute()
                .print();

    }
    /**
     * 自定义TableAggregateFunction
     * 1. 继承 TableAggregateFunction
     * 2. 实现多个方法 createAccumulator  accumulate  emitValue
     */
    @FunctionHint(output = @DataTypeHint("ROW<rank INTEGER, rk_vc INTEGER>" ))
    public static class MyTop2VcTableAggregateFunction extends TableAggregateFunction<Row, Tuple2<Integer, Integer>> {

        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            // Tuple2.of( 第一名的vc, 第二名的 vc )
            return Tuple2.of(Integer.MIN_VALUE, Integer.MIN_VALUE);
        }
        public void accumulate(Tuple2<Integer, Integer> acc, Integer value) {
            if(value > acc.f0) {
                acc.f1 = acc.f0;
                acc.f0 = value;
            } else if(value > acc.f1){
                acc.f1 = value;
            }
        }
        public void emitValue(Tuple2<Integer, Integer> acc, Collector<Row> out) {
            // 无条件输出第一名
            out.collect(Row.of(1, acc.f0));
            // 判断是否有第二名
            if(acc.f1 != Integer.MIN_VALUE){
                out.collect(Row.of(2, acc.f1));
            }
        }
    }
}

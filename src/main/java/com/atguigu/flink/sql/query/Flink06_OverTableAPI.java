package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.*;

/**
 * Over聚合
 *   1. TableAPI方式
 *
 *      1) OverWindow的使用
 *          Table resultTable = table
 *          .window([OverWindow w].as("w"))           // define over window with alias w
 *          .select($("a"), $("b").sum().over($("w")), $("c").min().over($("w"))); // aggregate over the over window w
 *
 *      2) OverWindow的创建
 *         Over.partitionBy($("a")).orderBy($("rowtime")).preceding(UNBOUNDED_RANGE).as("w")
 *   2） SQL方式
 *       1） 使用
 *       SELECT
 *         order_id, order_time, amount,
 *         SUM(amount) OVER ( PARTITION BY product ORDER BY order_time RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW ) AS one_hour_prod_amount_sum
 *       FROM Orders
 */
public class Flink06_OverTableAPI {
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

        // 基于行
        // 1. 上无边界，下无边界(错误的， 下界只能到当前行)
        //      报错内容    OVER RANGE FOLLOWING windows are not supported yet.
        OverWindow w1 =Over.partitionBy($("id")).orderBy($("pt")).preceding(UNBOUNDED_ROW).following(UNBOUNDED_ROW).as("w");

        // 2. 上无边界， 当前行
        OverWindow w2 = Over.partitionBy($("id")).orderBy($("pt")).preceding(UNBOUNDED_ROW).following(CURRENT_ROW).as("w");

        // 3. 上几行 ，当前行
        OverWindow w3 = Over.partitionBy($("id")).orderBy($("pt")).preceding(rowInterval(2L)).following(CURRENT_ROW).as("w");
        // 基于时间
        // 1. 上无边界，下无边界(错误的， 下界只能到当前行)
        // 2. 上无边界， 当前时间
            // 注意上面，会延迟2s
        OverWindow w4 = Over.partitionBy($("id")).orderBy($("pt")).preceding(UNBOUNDED_RANGE).following(CURRENT_RANGE).as("w");
        OverWindow w5 = Over.partitionBy($("id")).orderBy($("et")).preceding(UNBOUNDED_RANGE).following(CURRENT_RANGE).as("w");

        // 3. 上几秒， 当前时间
        OverWindow w6 = Over.partitionBy($("id")).orderBy($("et")).preceding(lit(2).seconds()).following(CURRENT_RANGE).as("w");

        // 使用方式
        table.window(w6)
                .select($("id"),$("vc"),$("ts"),$("vc").sum().over($("w")))
                .execute().print();

    }
}

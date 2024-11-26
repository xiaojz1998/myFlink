package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

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
public class Flink07_OverTableSQL {
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
        // OVER RANGE FOLLOWING windows are not supported yet.
        // 2. 上无边界， 当前行
        String sql1 =
                " select "+
                        " id,vc,ts, "+
                        " sum(vc) over( partition by id order by pt rows between unbounded preceding and current row) as sum_vc "+
                        " from t1 ";
        // 3. 上几行 ，当前行
        String sql2 =
                " select "+
                        " id,vc,ts, "+
                        " sum(vc) over( partition by id order by pt rows between 2 preceding and current row) as sum_vc "+
                        " from t1 ";
        // 基于时间
        // 1. 上无边界，下无边界(错误的， 下界只能到当前行)
        // 2. 上无边界， 当前时间
        String sql3 =
                " select "+
                        " id,vc,ts, "+
                        " sum(vc) over( partition by id order by pt range between unbounded preceding and current row) as sum_vc "+
                        " from t1 ";
        // 3. 上几秒， 当前时间
        String sql4 =
                " select "+
                        " id,vc,ts, "+
                        " sum(vc) over( partition by id order by et range between INTERVAL '2' SECOND preceding and current row) as sum_vc "+
                        " from t1 ";

        streamTableEnv.sqlQuery(sql4).execute().print();
    }
}

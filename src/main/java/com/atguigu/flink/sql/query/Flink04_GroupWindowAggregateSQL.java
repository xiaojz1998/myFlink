package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink04_GroupWindowAggregateSQL {
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
                .watermark("et" , "et - INTERVAL '0' SECOND ")
                .build();
        Table table = streamTableEnv.fromDataStream(ds, schema);
        streamTableEnv.createTemporaryView("t1" , table );
        // SQL
        // 时间窗口
        // 时间滚动窗口
        String sql1 =
                " SELECT " +
                        " id ," +
                        " SUM(vc)  as sum_vc ," +
                        " TUMBLE_START(pt , INTERVAL '10' SECOND) AS window_start,  "  +
                        " TUMBLE_END(pt , INTERVAL '10' SECOND) AS window_end " +
                        " FROM t1 " +
                        " GROUP BY TUMBLE( pt , INTERVAL '10' SECOND) , id " ;
        String sql2 =
                " SELECT " +
                        " id ," +
                        " SUM(vc)  as sum_vc ," +
                        " TUMBLE_START(et , INTERVAL '10' SECOND) AS window_start,  "  +
                        " TUMBLE_END(et , INTERVAL '10' SECOND) AS window_end " +
                        " FROM t1 " +
                        " GROUP BY TUMBLE( et , INTERVAL '10' SECOND) , id " ;
        // 时间滑动窗口
        String sql3 =
                " SELECT " +
                        " id ," +
                        " SUM(vc)  as sum_vc ," +
                        " HOP_START(pt , INTERVAL '5' SECOND , INTERVAL '10' SECOND) AS window_start,  "  +
                        " HOP_END(pt , INTERVAL '5' SECOND , INTERVAL '10' SECOND) AS window_end " +
                        " FROM t1 " +
                        " GROUP BY HOP( pt , INTERVAL '5' SECOND , INTERVAL '10' SECOND) , id " ;
        String sql4 =
                " SELECT " +
                        " id ," +
                        " SUM(vc)  as sum_vc ," +
                        " HOP_START(et , INTERVAL '5' SECOND , INTERVAL '10' SECOND) AS window_start,  "  +
                        " HOP_END(et , INTERVAL '5' SECOND , INTERVAL '10' SECOND) AS window_end " +
                        " FROM t1 " +
                        " GROUP BY HOP( et , INTERVAL '5' SECOND , INTERVAL '10' SECOND) , id " ;
        // 时间会话窗口
        String sql5 =
                " SELECT " +
                        " id ," +
                        " SUM(vc)  as sum_vc ," +
                        " SESSION_START(pt , INTERVAL '5' SECOND ) AS window_start,  "  +
                        " SESSION_END(pt , INTERVAL '5' SECOND ) AS window_end " +
                        " FROM t1 " +
                        " GROUP BY SESSION( pt , INTERVAL '5' SECOND ) , id " ;
        String sql6 =
                " SELECT " +
                        " id ," +
                        " SUM(vc)  as sum_vc ," +
                        " SESSION_START(et , INTERVAL '5' SECOND ) AS window_start,  "  +
                        " SESSION_END(et , INTERVAL '5' SECOND ) AS window_end " +
                        " FROM t1 " +
                        " GROUP BY SESSION( et , INTERVAL '5' SECOND ) , id " ;

        // 使用
        streamTableEnv.sqlQuery( sql6 )
                .execute()
                .print();


    }
}

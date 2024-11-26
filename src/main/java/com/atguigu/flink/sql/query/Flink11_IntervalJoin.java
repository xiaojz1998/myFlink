package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.OrderDetailEvent;
import com.atguigu.flink.pojo.OrderEvent;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * IntervalJoin:
 *     以一条流中数据的时间为基准， 设置一个上界和下界， 形成一个时间范围，
 *     另外一条流中相同key的数据只要落到对应的时间范围内，即可join成功。
 */
public class Flink11_IntervalJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //流表环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        // 流
        // OrderEvent: order-1,1000
        SingleOutputStreamOperator<OrderEvent> orderDs = env.socketTextStream("hadoop102", 8888)
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new OrderEvent(fields[0].trim(), Long.valueOf(fields[1].trim()));
                        }
                );
        // OrderDetailEvent : detail-1,order-1,1000
        SingleOutputStreamOperator<OrderDetailEvent> detailDs = env.socketTextStream("hadoop102", 9999)
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new OrderDetailEvent(fields[0].trim(), fields[1].trim() ,  Long.valueOf(fields[2].trim()));
                        }
                );
        //流转表
        Schema orderSchema = Schema.newBuilder()
                .column("id" , "STRING")
                .column("ts" , "BIGINT")
                .columnByExpression("pt" , "PROCTIME()")
                .columnByExpression("et" , "TO_TIMESTAMP_LTZ(ts, 3)")
                .watermark("et" , "et - INTERVAL '2' SECOND ")
                .build();
        Table orderTable = streamTableEnv.fromDataStream(orderDs, orderSchema);
        streamTableEnv.createTemporaryView("order_info" , orderTable );

        Schema detailSchema = Schema.newBuilder()
                .column("id" , "STRING")
                .column("orderId" , "STRING")
                .column("ts" , "BIGINT")
                .columnByExpression("pt" , "PROCTIME()")
                .columnByExpression("et" , "TO_TIMESTAMP_LTZ(ts, 3)")
                .watermark("et" , "et - INTERVAL '2' SECOND ")
                .build();
        Table detailTable = streamTableEnv.fromDataStream(detailDs, detailSchema);
        streamTableEnv.createTemporaryView("order_detail" , detailTable );

        // IntervalJoin
        streamTableEnv.sqlQuery(
                " select oi.id oi_id , od.id od_id , od.orderId "+
                        " from order_info oi , order_detail od " +
                        " where oi.id = od.orderId "+
                        " and od.et between oi.et - interval '2' second and oi.et + interval '2' second "
        ).execute().print();
    }
}

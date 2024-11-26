package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.OrderDetailEvent;
import com.atguigu.flink.pojo.OrderEvent;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 *  RegularJoin :
 *     1. 内连接
 *          inner join on
 *     2. 外连接
 *         左外连接   left outer join on
 *         右外连接   right outer join on
 *         全外连接   full outer join on
 */
public class Flink10_RegularJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 流表转换
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        // 设置状态的生存时间


        // 流
        // OrderEvent: order-1,1000
        SingleOutputStreamOperator<OrderEvent> ds1 = env.socketTextStream("hadoop102", 8888)
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new OrderEvent(fields[0].trim(), Long.valueOf(fields[1].trim()));
                        }
                );

        // OrderDetailEvent : detail-1,order-1,1000
        SingleOutputStreamOperator<OrderDetailEvent> ds2 = env.socketTextStream("hadoop102", 9999)
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new OrderDetailEvent(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                        }
                );

        // 流转表
        Schema orderSchema = Schema.newBuilder()
                .column("id" , "STRING")
                .column("ts" , "BIGINT")
                .columnByExpression("pt" , "PROCTIME()")
                .columnByExpression("et" , "TO_TIMESTAMP_LTZ(ts, 3)")
                .watermark("et" , "et - INTERVAL '2' SECOND ")
                .build();
        Table orderTable = streamTableEnv.fromDataStream(ds1, orderSchema);
        streamTableEnv.createTemporaryView("order_info" , orderTable);

        Schema detailSchema = Schema.newBuilder()
                .column("id" , "STRING")
                .column("orderId" , "STRING")
                .column("ts" , "BIGINT")
                .columnByExpression("pt" , "PROCTIME()")
                .columnByExpression("et" , "TO_TIMESTAMP_LTZ(ts, 3)")
                .watermark("et" , "et - INTERVAL '2' SECOND ")
                .build();
        Table detailTable = streamTableEnv.fromDataStream(ds2, detailSchema);
        streamTableEnv.createTemporaryView("order_detail" , detailTable);

        // inner join
        //streamTableEnv.sqlQuery(
        //        " select " +
        //                " oi.id , od.id , od.orderId " +
        //                " from order_info oi join order_detail od " +
        //                " on oi.id = od.orderId "
        //).execute().print();

        // outer join
        streamTableEnv.sqlQuery(
                " select " +
                        " oi.id , od.id , od.orderId " +
                        " from order_info oi left join order_detail od " +
                        " on oi.id = od.orderId "
        ).execute().print();

        //streamTableEnv.sqlQuery(
        //        " select " +
        //                " oi.id , od.id , od.orderId " +
        //                " from order_info oi full join order_detail od " +
        //                " on oi.id = od.orderId "
        //).execute().print();

    }
}

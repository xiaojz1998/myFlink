package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.ClickEvent;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * LookupJoin:  一条流与外部数据库的一张表的join。
 *              流中的每条数据都要进行一次join操作。 一般都是与外部的维度表进行join。
 *
 *  eg 在本例中则是和mysql中的数据join
 */
public class Flink12_LookUpJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //流表环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        // 流
        // ClickEvent : 1001,1,1000
        SingleOutputStreamOperator<ClickEvent> ds = env.socketTextStream("hadoop102", 8888)
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new ClickEvent(fields[0].trim(),  fields[1].trim() , Long.valueOf(fields[2].trim()));
                        }
                );

        //流转表
        Schema schema = Schema.newBuilder()
                .column("id" , "STRING")
                .column("provinceId" , "STRING")
                .column("ts" , "BIGINT")
                .columnByExpression("pt" , "PROCTIME()")
                .columnByExpression("et" , "TO_TIMESTAMP_LTZ(ts, 3)")
                .watermark("et" , "et - INTERVAL '2' SECOND ")
                .build();
        Table clickTable = streamTableEnv.fromDataStream(ds, schema);
        streamTableEnv.createTemporaryView("click_table" , clickTable  );

        // 维度表
        String provinceSql =
                " create table t_province ( " +
                        " id STRING , " +
                        " name STRING " +
                        " ) WITH ( " +
                        " 'connector' = 'jdbc', " +
                        " 'driver' = 'com.mysql.cj.jdbc.Driver' , " +
                        " 'url' = 'jdbc:mysql://hadoop102:3306/test' ,  " +
                        " 'username' = 'root' , " +
                        " 'password' = '000000' , " +
                        " 'table-name' = 't_province' ,  " +
                        " 'lookup.cache' = 'PARTIAL' , " +
                        " 'lookup.partial-cache.max-rows' = '50' ," +
                        " 'lookup.partial-cache.expire-after-write' = '20 SECOND' , " +
                        " 'lookup.partial-cache.expire-after-access' = '20 SECOND'  " +
                        " )" ;
        streamTableEnv.executeSql(provinceSql);
        // look up join
        streamTableEnv.sqlQuery(
                " select * " +
                        " from click_table as ct "+
                        " JOIN t_province FOR SYSTEM_TIME AS OF ct.pt AS tp "+
                        " ON ct.provinceId = tp.id "
        ).execute().print();
    }
}

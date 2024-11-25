package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

// 简单查询
public class Flink01_SimpleQuery {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 流表环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        // 流
        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new WaterSensor(fields[0].trim(), Integer.valueOf(fields[1].trim()), Long.valueOf(fields[2].trim()));
                        }
                );
        // 流转表
        Schema schema = Schema.newBuilder()
                .column("id" , "STRING")
                .column("vc" ,"INT")
                .column("ts" , "BIGINT")
                .columnByExpression("pt" , "PROCTIME()")
                .columnByExpression("et" , "TO_TIMESTAMP_LTZ(ts, 3)")
                .watermark("et" , "et - INTERVAL '0' SECOND ")
                .build();
        Table resultTable = streamTableEnv.fromDataStream(ds, schema);
        //resultTable.printSchema();
        streamTableEnv.createTemporaryView("t1",resultTable);

        // 简单查询
        // 1. select where
        //streamTableEnv.sqlQuery("select id, vc , ts from t1 where vc >=100").execute().print();

        // 2. with 字句，CTE
        // streamTableEnv.sqlQuery(" with t2 as (select id, vc , ts from t1 where vc >=100) select * from t2").execute().print();

        // 3. 去重
        // streamTableEnv.sqlQuery("select distinct id from t1").execute().print();

        // 4. order by 和 limit
        // order by : Sort on a non-time-attribute field is not supported.
        // order by : 只能使用时间属性字段 且 只能是升序。
        // LIMIT : 只支持Batch模式
        //streamTableEnv.sqlQuery("select id , vc , ts from t1 order by et asc").execute().print();

        // 5. sql hits
        // 用sql注释来改变原始表的定义
        String sourceTable =
                "create table t3 ( " +
                        " id STRING," +
                        " vc INT , " +
                        " ts BIGINT " +
                        " ) WITH ( " +
                        " 'connector' = 'kafka', " +
                        " 'topic' = 'topicA' , " +
                        " 'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092' ,  " +
                        " 'properties.group.id' = 'flinksql240620' , " +
                        " 'format' = 'csv' , " +
                        " 'scan.startup.mode' = 'latest-offset' " +
                        " ) " ;
        streamTableEnv.executeSql( sourceTable ) ;
        //streamTableEnv.sqlQuery( " select id , vc , ts from t3 /*+ OPTIONS('topic'='topicB') */").execute().print();

        // 6. 集合操作
        // 创建两个单列的表，用于操作
        streamTableEnv.executeSql("create view t4(s) as values ('c'), ('a'), ('b'), ('b'), ('c')");
        streamTableEnv.executeSql("create view t5(s) as values ('d'), ('e'), ('a'), ('b'), ('b')");

        // union去重 和 union all不去重
        //streamTableEnv.sqlQuery("select s from t4 union select s from t5").execute().print();
        //streamTableEnv.sqlQuery("select s from t4 union all select s from t5").execute().print();

        // INTERSECT 和 INTERSECT All 交集
        //streamTableEnv.sqlQuery(" select s from t4 INTERSECT select s from t5 ").execute().print();
        //streamTableEnv.sqlQuery(" select s from t4 INTERSECT ALL select s from t5 ").execute().print();

        // EXCEPT 和 EXCEPT All 差集
        //streamTableEnv.sqlQuery(" select s from t4 EXCEPT select s from t5 ").execute().print();
        //streamTableEnv.sqlQuery(" select s from t4 EXCEPT ALL select s from t5 ").execute().print();

        // 7. 分组聚合
        //streamTableEnv.sqlQuery("select sum(vc) as sum_vc from t1").execute().print();
        streamTableEnv.sqlQuery("select id,sum(vc) as sum_vc from t1 group by id").execute().print();
    }
}

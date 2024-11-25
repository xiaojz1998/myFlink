package com.atguigu.flink.sql.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink05_JdbcConnector {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(2000L);

        //流表环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        // jdbc source
        String sourceTable =
                "create table t_source ( " +
                        " id STRING," +
                        " vc INT , " +
                        " ts BIGINT " +
                        " ) WITH ( " +
                        " 'connector' = 'jdbc', " +
                        " 'driver' = 'com.mysql.cj.jdbc.Driver' , " +
                        " 'url' = 'jdbc:mysql://hadoop102:3306/test' ,  " +
                        " 'username' = 'root' , " +
                        " 'password' = '000000' , " +
                        " 'table-name' = 't_ws' " +
                        " ) " ;

        streamTableEnv.executeSql(sourceTable);
        // 查询数据
        Table resultTable = streamTableEnv.sqlQuery("select id,vc,ts from t_source");
        //resultTable.execute().print();
        streamTableEnv.createTemporaryView("t1", resultTable);
        //将表的结果写出到Mysql
        //String sinkTable =
        //        "create table t_sink_ws ( " +
        //                " id STRING," +
        //                " vc INT , " +
        //                " ts BIGINT , " +
        //                " PRIMARY KEY (id) NOT ENFORCED " +
        //        " ) WITH ( " +
        //                " 'connector' = 'jdbc', " +
        //                " 'driver' = 'com.mysql.cj.jdbc.Driver' , " +
        //                " 'url' = 'jdbc:mysql://hadoop102:3306/test' ,  " +
        //                " 'username' = 'root' , " +
        //                " 'password' = '000000' , " +
        //                " 'table-name' = 't_sink_ws1' " +
        //        " ) " ;

        String sinkTable =
                "create table t_sink_ws ( " +
                        " id STRING," +
                        " sum_vc INT , " +
                        " PRIMARY KEY (id) NOT ENFORCED " +
                        " ) WITH ( " +
                        " 'connector' = 'jdbc', " +
                        " 'driver' = 'com.mysql.cj.jdbc.Driver' , " +
                        " 'url' = 'jdbc:mysql://hadoop102:3306/test' ,  " +
                        " 'username' = 'root' , " +
                        " 'password' = '000000' , " +
                        " 'table-name' = 't_sink_ws2' " +
                        " ) " ;
        streamTableEnv.executeSql(sinkTable);
        streamTableEnv.executeSql("insert into t_sink_ws select id,sum(vc) from t1 group by id");


    }
}

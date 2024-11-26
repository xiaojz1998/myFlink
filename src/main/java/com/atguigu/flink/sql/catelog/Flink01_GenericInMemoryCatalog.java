package com.atguigu.flink.sql.catelog;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

// catalog: 管理元数据信息。
public class Flink01_GenericInMemoryCatalog {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 流表环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        // 获取当前使用的catalog
        // default_catalog  = GenericInMemoryCatalog
        String currentCatalog = streamTableEnv.getCurrentCatalog();
        System.out.println("currentCatalog = " + currentCatalog);

        // 获取当前使用的database
        // default_database
        String currentDatabase = streamTableEnv.getCurrentDatabase();
        System.out.println("currentDatabase = " + currentDatabase);

        //FileSource
        String sourceTable =
                "create table t_source ( " +
                        " id STRING," +
                        " vc INT , " +
                        " ts BIGINT  " +
                        " ) WITH ( " +
                        " 'connector' = 'filesystem', " +
                        " 'path' = 'input/ws.txt' , " +
                        " 'format' = 'csv' " +
                        " ) " ;
        streamTableEnv.executeSql( sourceTable ) ;

        //查询
        streamTableEnv.sqlQuery( " select id, vc ,ts from default_catalog.default_database.t_source ").execute().print();

    }
}

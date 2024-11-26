package com.atguigu.flink.sql.catelog;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.util.Arrays;

/**
 * HiveCatalog: 管理元数据信息。 可以读取Hive中的元数据 及 表的数据 , 且 支持将 Flink创建的表的元数据存储到Hive中。
 */
public class Flink03_HiveCatalog {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","atguigu");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 流表环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        // 使用HiveCatalog
        // 创建HiveCatalog对象
        HiveCatalog hiveCatalog = new HiveCatalog(
                "hiveCatalog",
                "default",
                "conf",
                "3.1.3"
        );

        //注册catalog
        streamTableEnv.registerCatalog("hiveCatalog" , hiveCatalog);

        //使用catalog
        streamTableEnv.useCatalog( "hiveCatalog" );

        // 获取当前使用的catalog
        String currentCatalog = streamTableEnv.getCurrentCatalog();
        System.out.println("currentCatalog = " + currentCatalog);

        // 获取当前使用的database
        String currentDatabase = streamTableEnv.getCurrentDatabase();
        System.out.println("currentDatabase = " + currentDatabase);

        //获取当前库下所有的表
        String[] tables = streamTableEnv.listTables();
        System.out.println(Arrays.toString( tables ));

        // 查询当前库下表的数据
        streamTableEnv.sqlQuery( " select * from student").execute().print();

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
        //streamTableEnv.executeSql( sourceTable ) ;

        //查询
        streamTableEnv.sqlQuery( " select id, vc ,ts from t_source ").execute().print();
    }
}

package com.atguigu.flink.sql.catelog;

import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

// JdbcCatalog: 管理元数据信息。 可以读取mysql中的元数据 及 表的数据
public class Flink02_JdbcCatalog {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 流表环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        // 使用JdbcCatalog
        // 创建Catalog对象
        JdbcCatalog jdbcCatalog = new JdbcCatalog(
                Flink02_JdbcCatalog.class.getClassLoader(),
                "myJdbcCatalog",
                "test",
                "root",
                "000000",
                "jdbc:mysql://hadoop102:3306"
        );

        //注册catalog
        streamTableEnv.registerCatalog("jdbcCatalog" , jdbcCatalog);

        //使用catalog
        streamTableEnv.useCatalog( "jdbcCatalog" );

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
        //streamTableEnv.sqlQuery( " select * from url_view_count").execute().print();

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
        // 异常:  Could not execute CreateTable in path `jdbcCatalog`.`test`.`t_source`
        //streamTableEnv.executeSql( sourceTable ) ;

        //查询
        //streamTableEnv.sqlQuery( " select id, vc ,ts from default_catalog.default_database.t_source ").execute().print();

    }
}

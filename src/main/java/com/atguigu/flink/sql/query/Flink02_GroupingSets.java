package com.atguigu.flink.sql.query;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.stream.Stream;

/**
 * 分组聚合的多维分析
 *
 * 需求: 有一张表 t_test , 字段如下: A  B  C  amount  ,
 *      其中 A , B , C 为三个维度字段， amount 为度量字段 ，统计 A , B , C 三个维度任意组合的 sum(amount )
 *
 * 1) UNION
 *    select A , null , null , sum(amount) from t_test group by A
 *    UNION
 *    select null , B , null , sum(amount) from t_test group by B
 *    UNION
 *    select null, null , C , sum(amount) from t_test group by C
 *    UNION
 *    select A , B , null , sum(amount) from t_test group by A , B
 *    UNION
 *    select A , null , C , sum(amount) from t_test group by A , C
 *    UNION
 *    select null ,  B , C , sum(amount) from t_test group by B , C
 *    UNION
 *    select A , B , C , sum(amount) from t_test group by A , B , C
 *    UNION
 *    select null ,null ,null ,sum(amount) from t_test
 *
 * 2) Flink 多维分析:
 *    GROUPING SETS :
 *    select A , B , C , sum(amount) from t_test GROUP BY GROUPING SETS( A , B , C , (A,B) , (A,C) , (B,C), (A,B,C) , ())
 *
 *    CUBE:
 *    select A , B , C , sum(amount) from t_test GROUP BY CUBE( A , B , C )
 *    等价于
 *    select A , B , C , sum(amount) from t_test GROUP BY GROUPING SETS( A , B , C , (A,B) , (A,C) , (B,C), (A,B,C) , ())
 *
 *    ROLLUP:
 *    select A , B , C , sum(amount) from t_test GROUP BY ROLLUP( A , B , C )
 *    等价于
 *    select A , B , C , sum(amount) from t_test GROUP BY GROUPING SETS(  () , A , (A,B) , (A,B,C))
 *
 * 3) Hive多维分析:
 *
 *     https://cwiki.apache.org/confluence/display/Hive/Enhanced+Aggregation%2C+Cube%2C+Grouping+and+Rollup
 *
 */
public class Flink02_GroupingSets {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //流表环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        // GROUPING SETS
        //streamTableEnv.sqlQuery(
        //        "SELECT A, B, C, sum(amount) AS total " +
        //                "FROM (VALUES " +
        //                "    ('A1', 'B1', 'C1' , 100 ), " +
        //                "    ('A2', 'B2', 'C2' , 200 )," +
        //                "    ('A3', 'B3', 'C3' , 300 )," +
        //                "    ('A1', 'B2', 'C2' , 400 )," +
        //                "    ('A1', 'B3', 'C3' , 500 )," +
        //                "    ('A2', 'B1', 'C2' , 600 ) " +
        //                " )" +
        //                "AS t1(A, B, C,amount) " +
        //                "GROUP BY GROUPING SETS (A , B , C , (A,B) , (A,C) , (B,C), (A,B,C) , ())"
        //).execute().print();

        // cube a,b,ab,() 他是程序自己生成组合 grouping sets是自己列举
        streamTableEnv.sqlQuery(
                "SELECT A, B,  sum(amount) AS total " +
                        "FROM (VALUES " +
                        "    ('A1', 'B1', 'C1' , 100 ), " +
                        "    ('A2', 'B2', 'C2' , 200 )," +
                        "    ('A3', 'B3', 'C3' , 300 )," +
                        "    ('A1', 'B2', 'C2' , 400 )," +
                        "    ('A1', 'B3', 'C3' , 500 )," +
                        "    ('A2', 'B1', 'C2' , 600 ) " +
                        " )" +
                        "AS t1(A, B, C,amount) " +
                        "GROUP BY CUBE ( A , B  )"
        ).execute().print();

        // roll_up 从后往前cba,ba,a,()
        //streamTableEnv.sqlQuery(
        //        "SELECT A, B, C, sum(amount) AS total " +
        //                "FROM (VALUES " +
        //                "    ('A1', 'B1', 'C1' , 100 ), " +
        //                "    ('A2', 'B2', 'C2' , 200 )," +
        //                "    ('A3', 'B3', 'C3' , 300 )," +
        //                "    ('A1', 'B2', 'C2' , 400 )," +
        //                "    ('A1', 'B3', 'C3' , 500 )," +
        //                "    ('A2', 'B1', 'C2' , 600 ) " +
        //                " )" +
        //                "AS t1(A, B, C,amount) " +
        //                "GROUP BY ROLLUP ( A , B , C )"
        //).execute().print();
    }
}

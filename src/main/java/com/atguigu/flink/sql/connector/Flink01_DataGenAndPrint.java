package com.atguigu.flink.sql.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @date 2024/11/25 8:57
 *
 * DataGenConnector : 用于模拟生成数据
 * PrintConnect: 用于输出
 */
public class Flink01_DataGenAndPrint {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 表环境
        //TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());
        // 流表环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        // DataGen
        String sourceTable =
                "CREATE TABLE t_source (" +
                        "    id STRING," +
                        "    vc INT," +
                        "    ts BIGINT" +
                        ") WITH (" +
                        "  'connector' = 'datagen'," +
                        "'rows-per-second'='1',"+
                        "'number-of-rows' = '1000',"+
                        "'fields.id.kind' = 'random',"+
                        "'fields.id.length' = '6',"+
                        "'fields.vc.kind' = 'random',"+
                        "'fields.vc.min' = '500',"+
                        "'fields.vc.max' = '1000',"+
                        "'fields.ts.kind' = 'sequence',"+
                        "'fields.ts.start' = '10000',"+
                        "'fields.ts.end' = '99999'"+
                        ")";
        streamTableEnv.executeSql(sourceTable);
        // 查询数据
        Table resultTable = streamTableEnv.sqlQuery("select id,vc,ts from t_source where vc >= 700");
        streamTableEnv.createTemporaryView("t_result", resultTable);

        // 将表的结果输出到控制台
        // print
        String sinkTable =
                "CREATE TABLE t_sink (" +
                        "    id STRING," +
                        "    vc INT," +
                        "    ts BIGINT" +
                        ") WITH (" +
                        "'connector' = 'print'," +
                        "'print-identifier'='PT',"+
                        "'standard-error' ='false',"+
                        "'sink.parallelism'='1'"+
                        ")";
        streamTableEnv.executeSql(sinkTable);
        // 写出
        //streamTableEnv.executeSql("insert into t_sink select id,vc,ts from t_result");
        resultTable.executeInsert("t_sink");
    }
}

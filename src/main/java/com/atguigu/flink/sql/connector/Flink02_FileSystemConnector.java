package com.atguigu.flink.sql.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * file source + file sink
 */
public class Flink02_FileSystemConnector {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 流表环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        // file Source
        String sourceTable =
                "CREATE TABLE t_source (" +
                        "  id STRING," +
                        "  vc INT," +
                        "  ts BIGINT," +
                        //" `file.path` STRING NOT NULL METADATA , " +
                        //" `file.name` STRING NOT NULL METADATA , " +
                        " `file.size` BIGINT NOT NULL METADATA , " +
                        " `file.modification-time` TIMESTAMP_LTZ(3) NOT NULL METADATA " +
                        ") WITH (" +
                        "  'connector' = 'filesystem'," +
                        "  'path' = 'input/ws.txt'," +
                        "  'format' = 'csv'" +
                        ")";

        streamTableEnv.executeSql(sourceTable);
        // 查询数据
        Table resultTable = streamTableEnv.sqlQuery("select id,vc,ts,`file.size`,`file.modification-time` from t_source");
        // 测试打印数据
        //resultTable.execute().print();
        streamTableEnv.createTemporaryView("t1", resultTable);

        // 将表的结果写出到文件
        String sinkTable =
                "CREATE TABLE t_sink (" +
                        "  id STRING," +
                        "  vc INT," +
                        "  ts BIGINT," +
                        "  fs BIGINT , " +
                        " fmt TIMESTAMP_LTZ(3)" +
                        ") WITH (" +
                        "  'connector' = 'filesystem'," +
                        "  'path' = 'output'," +
                        "  'format' = 'json'" +
                        ")";
        streamTableEnv.executeSql(sinkTable);
        // 写出
        resultTable.executeInsert("t_sink");

    }
}

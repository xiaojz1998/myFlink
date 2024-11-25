package com.atguigu.flink.sql.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink03_KafkaConnector {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(2000l);

        // 流表环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        // kafka source
        String sourceTable =
                "CREATE TABLE t_source (" +
                        "  `id` String," +
                        "  `vc` INT," +
                        "  `ts` BIGINT," +
                        " `topic` STRING NOT NULL METADATA  , " +
                        " `partition` INT NOT NULL METADATA , " +
                        " `offset` BIGINT NOT NULL METADATA " +
                        ") WITH (" +
                        "  'connector' = 'kafka'," +
                        "  'topic' = 'flink'," +
                        "  'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092'," +
                        "  'properties.group.id' = 'myFlink'," +
                        "  'scan.startup.mode' = 'latest-offset'," +
                        "  'format' = 'csv'" +
                        //" 'properties.配置项' = '配置值'"  其他配置使用该方式来配置
                        ")";
        streamTableEnv.executeSql(sourceTable);

        // 查询数据
        Table resultTable = streamTableEnv.sqlQuery("select id,vc,ts,`topic`,`partition`,`offset` from t_source");
        // 测试
        //resultTable.execute().print();
        streamTableEnv.createTemporaryView("t1", resultTable);

        // 将表的结果写到kafka
        String sinkTable =
                "create table t_sink ( " +
                        " id STRING," +
                        " vc INT , " +
                        " ts BIGINT, " +
                        " tp STRING , " +
                        " pt INT , " +
                        " ot BIGINT " +
                        " ) WITH ( " +
                        " 'connector' = 'kafka', " +
                        " 'topic' = 'first' , " +
                        " 'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092' ,  " +
                        " 'format' = 'json' , " +
                        //" 'sink.delivery-guarantee' = 'at-least-once' " +
                        " 'sink.delivery-guarantee' = 'exactly-once' , " +
                        " 'sink.transactional-id-prefix' = 'flink-" +System.currentTimeMillis()+ "'," +
                        " 'properties.transaction.timeout.ms' = '600000' " +
                        //" 'sink.parallelism' = '1'"
                        " ) " ;
        streamTableEnv.executeSql(sinkTable);
        //resultTable.executeInsert("t_sink");
        streamTableEnv.executeSql("insert into t_sink select id,vc,ts,`topic`,`partition`,`offset` from t1");
    }
}

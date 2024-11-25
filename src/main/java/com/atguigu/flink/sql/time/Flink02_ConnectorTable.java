package com.atguigu.flink.sql.time;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * 连接器表定时时间属性字段
 */
public class Flink02_ConnectorTable {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 流表环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        //连接器表
        String SourceTable=
                "create table t_source("+
                        "id STRING,"+
                        "vc INT,"+
                        "ts BIGINT,"+
                        // 处理时间字段
                        "pt AS PROCTIME(),"+
                        // 事件时间字段
                        "et AS TO_TIMESTAMP_LTZ(ts,3),"+
                        "WATERMARK FOR et AS et - INTERVAL '2' SECOND"+
                        ") WITH (" +
                        " 'connector' = 'kafka',"+
                        " 'topic' = 'topicA',"+
                        " 'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092' ,  "+
                        " 'properties.group.id' = 'myflink',"+
                        " 'format' = 'csv',"+
                        " 'scan.startup.mode'='latest-offset'"+")";
        streamTableEnv.executeSql(SourceTable);
        Table t_source = streamTableEnv.from("t_source");
        t_source.printSchema();
    }
}

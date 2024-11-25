package com.atguigu.flink.sql.time;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 流转表定义时间属性字段
 */
public class Flink01_StreamToTable {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(
                        line -> {
                            String[] sl = line.split(",");
                            return new WaterSensor(sl[0].trim(), Integer.valueOf(sl[1].trim()), Long.valueOf(sl[2].trim()));
                        }
                ).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        (element, ts) -> element.getTs()
                                )
                );

        // 流表环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        // 流转表
        // 创建schema
        Schema schema = Schema.newBuilder()
                //.column("id", DataTypes.STRING())
                .column("id", "STRING")
                .column("vc", "INT")
                .column("ts", "BIGINT")
                // 处理时间字段
                .columnByExpression("pt", "PROCTIME()") //  *PROCTIME*
                // 事件时间字段
                .columnByExpression("et", "TO_TIMESTAMP_LTZ(ts,3)") // *ROWTIME*
                // 生成水位线
                // 如果流中已经生成过水位线，可以直接沿用流中的水位线
                //.watermark("et","source_watermark()")
                // 定义水位线的生成
                .watermark("et", "et - INTERVAL '2' SECOND")
                .build();
        Table table = streamTableEnv.fromDataStream(ds, schema);
        table.printSchema();

    }
}

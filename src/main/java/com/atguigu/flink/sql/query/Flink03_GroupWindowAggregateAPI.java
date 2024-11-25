package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static  org.apache.flink.table.api.Expressions.*;


/**
 * 1. 分组窗口聚合
 *     1） 支持的窗口类型
 *          滚动窗口
 *          滑动窗口
 *          会话窗口
 *     2） 支持 TableAPI 以及 SQL形式
 *         TableAPI:
 *             窗口的使用： Table resultTable = table
 *                                      .window([GroupWindow w].as("w"))  // define window with alias w
 *                                      .groupBy($("w"))  // group the table by window w
 *                                      .select($("b").sum());  // aggregate
 *             窗口的创建:
 *                       Tumble.over(lit(10).minutes()).on($("rowtime")).as("w")
 *                       Slide.over(rowInterval(10)).every(rowInterval(5)).on($("proctime")).as("w")
 *                       Session.withGap(lit(10).minutes()).on($("rowtime")).as("w")
 * 2. Window TVF 聚合 （窗口表值函数）
 *     1) 支持的窗口类型
 *          滚动窗口
 *          滑动窗口
 *          累积窗口
 *          会话窗口(未来会支持)
 *     2） 只支持SQL形式
 *     3） 相对于分组窗口聚合， WindowTVF的优点:
 *          提供更多的性能优化手段
 *          支持GroupingSets语法
 *          可以在window聚合中使用TopN
 *          支持累积窗口
 */
public class Flink03_GroupWindowAggregateAPI {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 流表环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        // 流
        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new WaterSensor(fields[0].trim(), Integer.valueOf(fields[1].trim()), Long.valueOf(fields[2].trim()));
                        }
                );
        // 流转表
        Schema schema = Schema.newBuilder()
                .column("id" , "STRING")
                .column("vc" ,"INT")
                .column("ts" , "BIGINT")
                .columnByExpression("pt","PROCTIME()")
                .columnByExpression("et","TO_TIMESTAMP_LTZ(ts,3)")
                .columnByExpression("et","et - INTERVAL '0' SECOND")
                .build();
        Table table = streamTableEnv.fromDataStream(ds, schema);
        // Table API
        // 计数窗口
        // 计数滚动窗口
        //  Event-time grouping windows on row intervals are currently not supported.
        TumbleWithSizeOnTimeWithAlias w1 = Tumble.over(rowInterval(3l)).on($("pt")).as("w");

        // 计数滑动窗口
        // 注意: 窗口的首次计算必须满足窗口大小， 后续计算按照滑动步长计算。
        SlideWithSizeAndSlideOnTimeWithAlias w2 = Slide.over(rowInterval(3l)).every(rowInterval(2l)).on($("pt")).as("w");

        // 时间窗口
        // 时间滚动窗口
        TumbleWithSizeOnTimeWithAlias w3 = Tumble.over(lit(10).second()).on($("et")).as("w");
        TumbleWithSizeOnTimeWithAlias w4 = Tumble.over(lit(10).second()).on($("pt")).as("w");

        // 时间滑动窗口
        SlideWithSizeAndSlideOnTimeWithAlias w5 = Slide.over(lit(10).second()).every(lit(5).second()).on($("et")).as("w");
        SlideWithSizeAndSlideOnTimeWithAlias w6 = Slide.over(lit(10).second()).every(lit(5).second()).on($("pt")).as("w");

        // 时间绘画窗口
        SessionWithGapOnTimeWithAlias w7 = Session.withGap(lit(5).second()).on($("et")).as("w");
        SessionWithGapOnTimeWithAlias w8 = Session.withGap(lit(5).second()).on($("pt")).as("w");

        // 使用
        table.window(w3)
                .groupBy($("w"),$("id"))
                .select($("id"),$("vc").sum().as("sum_vc"),$("w").start().as("w_start"),$("w").end().as("w_end"))
                .execute().print();
    }
}

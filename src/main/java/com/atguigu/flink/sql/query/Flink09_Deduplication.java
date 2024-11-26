package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * Deduplication : 特殊语法 ， 需要对排名后的结果进行过滤处理 （ where rk = 1 ）
 *              且 排序字段为时间属性字段 ,Flink才能识别为Deduplication操作。
 */
public class Flink09_Deduplication {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> ds = env.fromSource(SourceUtil.getSource(), WatermarkStrategy.noWatermarks(), "dataGenSource")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        (event, ts) -> event.getTs()
                                )
                );

        ds.print();

        //流表环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        //流转表
        Schema schema = Schema.newBuilder()
                .column("user", "STRING")
                .column("url", "STRING")
                .column("ts" , "BIGINT")
                .columnByExpression("pt" , "PROCTIME()")
                .columnByExpression("et" , "TO_TIMESTAMP_LTZ(ts,3)")
                .watermark("et", "source_watermark()")
                .build();
        Table table = streamTableEnv.fromDataStream(ds, schema);
        streamTableEnv.createTemporaryView("t1" , table );

        // 需求: 统计每个窗口中每个url最后到达的数据
        // 1. 首先开窗
        String windowSql =
                " select window_start, window_end , user ,  url , ts , et ,pt  " +
                        " from table ( "+
                        " tumble ( table t1 , descriptor(et) , interval '10' second ) "+
                        " ) " ;
        Table windowTable = streamTableEnv.sqlQuery(windowSql);
        streamTableEnv.createTemporaryView("window_table" , windowTable);
        // 2. 排序， 获取每个窗口内每个url的排名
        String rankSql =
                " select * , "+
                        " row_number() over ( partition by window_start, window_end , url order by et desc) as rk " +
                        " from window_table " ;
        Table rankSqlTable = streamTableEnv.sqlQuery(rankSql);
        streamTableEnv.createTemporaryView("rank_table" , rankSqlTable);
        // 3. 过滤， 只保留排名为1的数据
        String topNSql =
                " select * from rank_table where rk = 1 ";
        streamTableEnv.sqlQuery(topNSql).execute().print();
    }
}

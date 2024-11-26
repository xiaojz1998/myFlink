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
 *
 * TopN : 特殊语法 ， 需要对排名后的结果进行过滤处理 （ where rk <= N ） ,Flink才能识别为TopN操作， 支持在orderBy的后面出现非时间属性字段且支持降序。
 */
public class Flink08_TopN {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> ds = env.fromSource(SourceUtil.getSource(), WatermarkStrategy.noWatermarks(), "source")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        (e, t) -> e.getTs()
                                )
                );
        ds.print();
        // 流表环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        // 流转表
        Schema schema = Schema.newBuilder()
                .column("user","STRING")
                .column("url","STRING")
                .column("ts","BIGINT")
                .columnByExpression("pt","PROCTIME()")
                .columnByExpression("et","TO_TIMESTAMP_LTZ(ts,3)")
                .watermark("et","source_watermark()")
                .build();
        Table table = streamTableEnv.fromDataStream(ds, schema);
        streamTableEnv.createTemporaryView("t1" , table );
        //table.execute().print();

        // 需求: 统计每10秒内每个url的点击次数， 输出前2名
        //1. 通过窗口统计每个url的点击次数
        String windowSql =
                " select window_start,window_end,url,count(*) url_cnt "+
                        " from table("+
                        " tumble(table t1 , descriptor(et) , interval '10' second) "+
                        " ) " +
                        " group by window_start, window_end , url ";
        Table windowTable = streamTableEnv.sqlQuery(windowSql);
        //windowTable.execute().print();
        streamTableEnv.createTemporaryView("window_table" , windowTable);


        // 2. 通过窗口的结束时间进行分区， 按照点击次数排序， 求排名
        // The window can only be ordered in ASCENDING mode.
        // OVER windows' ordering in stream mode must be defined on a time attribute.
        String rankSql =
                " select " +
                        " window_start , window_end , url , url_cnt ," +
                        " row_number() over ( partition by window_start, window_end order by url_cnt desc) as rk " +
                        " from window_table ";
        Table rankSqlTable = streamTableEnv.sqlQuery(rankSql);
        streamTableEnv.createTemporaryView("rank_table" , rankSqlTable);

        // 3. 对排名的过滤
        String topNSql =
                " select " +
                        " window_start , window_end , url , url_cnt , rk "+
                        " from rank_table "+
                        " where rk <= 2 ";
        Table topNTable = streamTableEnv.sqlQuery(topNSql);
        topNTable.execute().print();

    }
}

package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 1. 分组窗口聚合
 *     1） 支持的窗口类型
 *          滚动窗口
 *          滑动窗口
 *          会话窗口
 *     2） 支持 TableAPI 以及 SQL形式
 *         TableAPI: 支持计数和时间窗口
 *             窗口的使用： Table resultTable = table
 *                                      .window([GroupWindow w].as("w"))  // define window with alias w
 *                                      .groupBy($("w"))  // group the table by window w
 *                                      .select($("b").sum());  // aggregate
 *
 *             窗口的创建:
 *                       Tumble.over(lit(10).minutes()).on($("rowtime")).as("w")
 *                       Slide.over(rowInterval(10)).every(rowInterval(5)).on($("proctime")).as("w")
 *                       Session.withGap(lit(10).minutes()).on($("rowtime")).as("w")
 *
 *        SQL : 只支持时间窗口
 *          窗口的使用:
 *                SELECT
 *                user,
 *                TUMBLE_START(order_time, INTERVAL '1' DAY) AS wStart,
 *                SUM(amount) FROM Orders
 *              GROUP BY
 *                TUMBLE(order_time, INTERVAL '1' DAY),
 *                user
 *          窗口的创建:
 *              TUMBLE(time_attr, interval)
 *              HOP(time_attr, interval, interval)
 *              SESSION(time_attr, interval)
 *
 * 2. Window TVF 聚合 （窗口表值函数）
 *     1) 支持的窗口类型
 *          滚动窗口
 *          滑动窗口
 *          累积窗口
 *              you could have a cumulating window for 1 hour step and 1 day max size,
 *              and you will get windows:
 *              [00:00, 01:00),
 *              [00:00, 02:00),
 *              [00:00, 03:00),
 *              …,
 *              [00:00, 24:00)
 *              for every day.
 *
 *          会话窗口(未来会支持)
 *
 *     2） 只支持SQL形式
 *         使用窗口:
 *          SELECT
 *             window_start, window_end, SUM(price)
 *          FROM TABLE(
 *            TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES)
 *            )
 *          GROUP BY window_start, window_end;
 *
 *     3） 相对于分组窗口聚合， WindowTVF的优点:
 *          提供更多的性能优化手段
 *          支持GroupingSets语法
 *          可以在window聚合中使用TopN
 *          支持累积窗口
 *
 */
public class Flink05_WindowTVF {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 流表环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        // 流
        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(
                        line -> {
                            String[] sl = line.split(",");
                            return new WaterSensor(sl[0].trim(), Integer.valueOf(sl[1].trim()), Long.valueOf(sl[2].trim()));
                        }
                );
        // 流转表
        Schema schema = Schema.newBuilder()
                .column("id","STRING")
                .column("vc","INT")
                .column("ts","BIGINT")
                .columnByExpression("pt","PROCTIME()")
                .columnByExpression("et","TO_TIMESTAMP_LTZ(ts,3)")
                .watermark("et","et - INTERVAL '0' SECOND")
                .build();
        Table table = streamTableEnv.fromDataStream(ds, schema);
        streamTableEnv.createTemporaryView("t1",table);

        // SQL
        // 时间窗口
        // 时间滚动窗口
        // 处理时间
        String sql1 =
                " select "+
                        " window_start, window_end,id,sum(vc) as sum_vc "+
                        " from table("+
                        " tumble(table t1 , descriptor(pt), interval '10' second)"+
                        " ) "+
                        " group by id,window_start,window_end ";
        // 事件时间
        String sql2 =
                " select "+
                        " window_start, window_end,id,sum(vc) as sum_vc "+
                        " from table("+
                        " tumble(table t1 , descriptor(et), interval '10' second)"+
                        " ) "+
                        " group by id,window_start,window_end ";

        // 时间滑动窗口
        // 处理时间
        String sql3 =
                " select "+
                        " window_start, window_end,id,sum(vc) as sum_vc "+
                        " from table( "+
                        " hop(table t1 , descriptor(pt), interval '5' second, interval '10' second)"+
                        " ) "+
                        " group by id,window_start,window_end ";
        // 事件时间
        String sql4 =
                " select "+
                        " window_start, window_end,id,sum(vc) as sum_vc "+
                        " from table( "+
                        " hop(table t1 , descriptor(et), interval '5' second, interval '10' second)"+
                        " ) "+
                        " group by id,window_start,window_end ";

        // 时间累计窗口
        String sql5 =
                " select "+
                        " window_start, window_end,id,sum(vc) as sum_vc "+
                        " from table( "+
                        " cumulate(table t1 , descriptor(pt), interval '2' second, interval '10' second)"+
                        " ) "+
                        " group by id,window_start,window_end ";
        String sql6 =
                " select "+
                        " window_start, window_end,id,sum(vc) as sum_vc "+
                        " from table( "+
                        " cumulate(table t1 , descriptor(et), interval '2' second, interval '10' second)"+
                        " ) "+
                        " group by id,window_start,window_end ";

        // 使用方法
        streamTableEnv.sqlQuery(sql6).execute().print();
    }
}

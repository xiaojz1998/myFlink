package com.atguigu.flink.timeandwindow;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.UrlViewCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;

/**
 *  迟到数据的结果修正
 */
public class Flink12_LateDataCorrect {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1).getConfig().setAutoWatermarkInterval(1000);

        // 开启检查点
        env.enableCheckpointing(2000);
        SingleOutputStreamOperator<Event> ds = env.socketTextStream("hadoop102", 8888)
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                        }

                ).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2)) // 1. 延迟水位线的推进 ，
                                .withTimestampAssigner(
                                        (event, ts) -> event.getTs()
                                )
                );
        ds.print("INPUT");
        OutputTag<Event> lateOutputTag = new OutputTag<>("lateOutputTag", Types.POJO(Event.class));

        // 需求: 统计每10秒内每个url的点击次数 ， 将统计的结果写出到Mysql表中
        SingleOutputStreamOperator<UrlViewCount> windowDs = ds.keyBy(
                Event::getUrl
        ).window(
                TumblingEventTimeWindows.of(Time.seconds(10))
        ).allowedLateness( Time.seconds(5) ) //  2. 延迟窗口的关闭
         .sideOutputLateData(lateOutputTag)  //  3. 使用侧输出流接收极端数据
        .aggregate(
                new AggregateFunction<Event, UrlViewCount, UrlViewCount>() {
                    @Override
                    public UrlViewCount createAccumulator() {
                        return new UrlViewCount(null, null, null, 0L);
                    }

                    @Override
                    public UrlViewCount add(Event value, UrlViewCount accumulator) {
                        //accumulator.setUrl( value.getUrl());
                        accumulator.setCount(accumulator.getCount() + 1);
                        return accumulator;
                    }

                    @Override
                    public UrlViewCount getResult(UrlViewCount accumulator) {
                        return accumulator;
                    }

                    @Override
                    public UrlViewCount merge(UrlViewCount a, UrlViewCount b) {
                        return null;
                    }
                },
                new ProcessWindowFunction<UrlViewCount, UrlViewCount, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<UrlViewCount, UrlViewCount, String, TimeWindow>.Context context, Iterable<UrlViewCount> elements, Collector<UrlViewCount> out) throws Exception {
                        // 获取增量聚合的结果
                        UrlViewCount urlViewCount = elements.iterator().next();
                        //设置url
                        urlViewCount.setUrl(key);
                        //设置窗口信息
                        urlViewCount.setWindowStart(context.window().getStart());
                        urlViewCount.setWindowEnd(context.window().getEnd());

                        //输出
                        out.collect(urlViewCount);
                    }
                }
        );

        //写出到Mysql表中
        SinkFunction<UrlViewCount> jdbcSink = JdbcSink.<UrlViewCount>sink(
                "replace into url_view_count (window_start ,window_end, url ,cnt ) values (?,?,?,?)",
                new JdbcStatementBuilder<UrlViewCount>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, UrlViewCount urlViewCount) throws SQLException {
                        // 给sql中的？赋值
                        preparedStatement.setLong(1, urlViewCount.getWindowStart());
                        preparedStatement.setLong(2, urlViewCount.getWindowEnd());
                        preparedStatement.setString(3, urlViewCount.getUrl());
                        preparedStatement.setLong(4, urlViewCount.getCount());
                    }
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://hadoop102:3306/test")
                        .withUsername("root")
                        .withPassword("000000")
                        .build()
        );
        // 将窗口计算的结果写出到Mysql表中
        windowDs.addSink(jdbcSink);
        // 捕获侧输出流
        SideOutputDataStream<Event> lateDs = windowDs.getSideOutput(lateOutputTag);
        //lateDs.print("lateDs");

        // 写到mysql中
        SinkFunction<UrlViewCount> lateJdbcSink = JdbcSink.<UrlViewCount>sink(
                "insert into url_view_count (window_start ,window_end, url ,cnt ) values (?,?,?,?) on duplicate key update cnt = cnt + VALUES(cnt) ",
                new JdbcStatementBuilder<UrlViewCount>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, UrlViewCount urlViewCount) throws SQLException {
                        //给sql中的?赋值
                        preparedStatement.setLong(1, urlViewCount.getWindowStart());
                        preparedStatement.setLong(2, urlViewCount.getWindowEnd());
                        preparedStatement.setString(3, urlViewCount.getUrl());
                        preparedStatement.setLong(4, urlViewCount.getCount());
                    }
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://hadoop102:3306/test")
                        .withUsername("root")
                        .withPassword("000000")
                        .build()
        );
        // 将侧输出流中的Event 数据 转换成 UrlViewCount格式
        lateDs.map(
                event -> {
                    // 通过event中的时间， 计算窗口的开始时间和结束时间
                    long windowStart = TimeWindow.getWindowStartWithOffset(event.getTs(), 0, 10 * 1000);
                    long windowEnd = windowStart + 10 * 1000;
                    return new UrlViewCount(windowStart,windowEnd,event.getUrl(),1L);
                }
        ).addSink(lateJdbcSink);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

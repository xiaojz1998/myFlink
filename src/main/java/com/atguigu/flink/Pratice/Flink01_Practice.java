package com.atguigu.flink.Pratice;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.UrlViewCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;

/**
 *
 * 编程题:
 *从Kafka topicA 主题中读取Event数据(json格式或者csv格式) ,
 *统计每10秒内每个URL的点击次数，将结果写出到Mysql url_view_count表中。
 *要求: 增量聚合函数和全窗口函数结合使用，输出结果包含窗口信息。
 */
public class Flink01_Practice {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(2000l);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setTopics("topicA")
                .setGroupId("flink")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        //写出到Mysql表中
        SinkFunction<UrlViewCount> jdbcSink = JdbcSink.<UrlViewCount>sink(
                "insert into url_view_count (window_start ,window_end, url ,cnt ) values (?,?,?,?)",
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

        SingleOutputStreamOperator<Event> ds = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource")
                .map(
                        e -> {
                            //System.out.println("source"+e);
                            String[] split = e.split(",");
                            return new Event(split[0], split[1], Long.parseLong(split[2]));
                        }
                ).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        (element, ts) -> element.getTs()
                                )
                );

        ds.print("INPUT");
        SingleOutputStreamOperator<UrlViewCount> aggregateDs = ds.keyBy(Event::getUrl)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(
                        new AggregateFunction<Event, UrlViewCount, UrlViewCount>() {
                            @Override
                            public UrlViewCount createAccumulator() {
                                return new UrlViewCount(null, null, null, 0L);
                            }

                            @Override
                            public UrlViewCount add(Event event, UrlViewCount urlViewCount) {
                                urlViewCount.setCount(urlViewCount.getCount() + 1);
                                return urlViewCount;
                            }

                            @Override
                            public UrlViewCount getResult(UrlViewCount urlViewCount) {
                                return urlViewCount;
                            }

                            @Override
                            public UrlViewCount merge(UrlViewCount urlViewCount, UrlViewCount acc1) {
                                return null;
                            }
                        },
                        new ProcessWindowFunction<UrlViewCount, UrlViewCount, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<UrlViewCount, UrlViewCount, String, TimeWindow>.Context context, Iterable<UrlViewCount> iterable, Collector<UrlViewCount> collector) throws Exception {
                                UrlViewCount urlViewCount = iterable.iterator().next();
                                urlViewCount.setUrl(s);
                                // 获取窗口信息
                                long start = context.window().getStart();
                                long end = context.window().getEnd();
                                // 设置窗口信息
                                urlViewCount.setWindowStart(start);
                                urlViewCount.setWindowEnd(end);
                                // 输出
                                collector.collect(urlViewCount);
                            }
                        }
                );
        aggregateDs.print("结果");
        aggregateDs.addSink(jdbcSink);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

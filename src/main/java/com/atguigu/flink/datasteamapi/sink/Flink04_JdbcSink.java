package com.atguigu.flink.datasteamapi.sink;


import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Flink04_JdbcSink {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //开启检查点
        env.enableCheckpointing(2000l);
        DataStreamSource<Event> ds = env.fromSource(SourceUtil.getSource(), WatermarkStrategy.noWatermarks(), "dataGenSource");

        // 将数据写入到mysql表中
        SinkFunction<Event> jdbcSinkFunction = JdbcSink.<Event>sink(
                //" insert into t_event1 (user, url ,ts ) values(? ,? ,?)",
                // 幂等写入
                //" replace into t_event1 (user, url ,ts ) values(? ,? ,?) " ,   // insert or update , 如果主键不存在，执行insert， 如果主键存在，执行所有非主键列的update操作
                " insert into t_event1 (user, url ,ts ) values(? ,? ,?) on duplicate key update url = VALUES(url)",
                new JdbcStatementBuilder<Event>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Event event) throws SQLException {
                        //给SQL的占位符赋值
                        preparedStatement.setString(1, event.getUser());
                        preparedStatement.setString(2, event.getUrl());
                        preparedStatement.setLong(3, event.getTs());
                    }
                },
                /*
                JdbcExecutionOptions.builder()
                        .withBatchSize(3)
                        .withBatchIntervalMs(3000)
                        .withMaxRetries(2)
                        .build() ,
                 */
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://hadoop102:3306/test")
                        .withUsername("root")
                        .withPassword("000000")
                        .build()
        );
        ds.addSink(jdbcSinkFunction);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

package com.atguigu.flink.sql.api;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

// 流表转换
public class Flink02_StreamTable {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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
        Table table = streamTableEnv.fromDataStream(ds);
        streamTableEnv.createTemporaryView("t1", table);
        // 对数据进行处理
        // 追加查询
        Table resultTable = streamTableEnv.sqlQuery(" select id , vc , ts from t1 where vc >= 100 ");
        // 更新查询
        //Table resultTable = streamTableEnv.sqlQuery(" select id, sum(vc) from t1 group by id ");
        // 表转流
        // 追加查询得到的表
        //DataStream<Row> resultDs = streamTableEnv.toDataStream(resultTable);
        DataStream<Row> resultDs = streamTableEnv.toChangelogStream(resultTable);

        // 更新查询得到的表
        // 异常: Table sink '*anonymous_datastream_sink$2*' doesn't support consuming update changes which is produced by node GroupAggregate(groupBy=[id], select=[id, SUM(vc) AS EXPR$1])
        //DataStream<Row> resultDs = streamTableEnv.toChangelogStream(resultTable);
        resultDs.print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

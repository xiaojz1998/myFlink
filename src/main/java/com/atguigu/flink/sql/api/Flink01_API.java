package com.atguigu.flink.sql.api;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Flink SQL 程序架构:
 *
 * 1. 准备执行环境
 *    1） 流表环境 StreamTableEnvironment ， 支持流表转换
 *    2） 表环境  TableEnvironment ，只能基于表操作
 *
 * 2. 创建表
 *    1） 使用流表环境进行流转表操作
 *    2） 使用表环境 或者 流表环境 通过连接器表的方式创建表
 *
 * 3. 对表中的数据进行转换处理
 *    1）TableAPI的方式
 *    2）SQL的方式
 *
 * 4. 输出
 *    1） 表转流 , 基于流进行输出
 *
 *    2） 连接器表输出
 */
public class Flink01_API {
    public static void main(String[] args) {
        // 1. 创建表执行环境
        // 创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1) 流表环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        // 2) 表环境
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());

        // 2. 创建表
        // 通过流转表的方式来建表
        // 输入内容格式：s1,100,1000
        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(
                        line -> {
                            String[] sl = line.split(",");
                            return new WaterSensor(sl[0].trim(), Integer.valueOf(sl[1].trim()), Long.valueOf(sl[2].trim()));
                        }
                );
        Table table = streamTableEnv.fromDataStream(ds);
        // 表转流
        // 将表转换成一个追加流（解析后是insert）
        //DataStream<Row> rowDataStream = streamTableEnv.toDataStream(table);
        // 将表转换成一个更新流（解析后是upset eg 有聚合操作）
        DataStream<Row> rowDataStream = streamTableEnv.toChangelogStream(table);

        // 2) 连接器表
        streamTableEnv.executeSql( " create table xxx ( 字段信息 ) with ( 连接器参数 )");
        tableEnv.executeSql( " create table xxx ( 字段信息 ) with ( 连接器参数)");

        //3. 对表中的数据进行转换处理
        // TableAPI方式
        //Table resultTalbe = table.where().join().orderBy().groupBy().select();
        Table resultTable = table.select($("id"), $("vc"), $("ts"));

        // sql 方式
        // 记住这里是sqlQuery 而不是executeSql
        streamTableEnv.createTemporaryView("t1", table);
        //Table resultTalbe = streamTableEnv.sqlQuery("select id,vc,ts from t1");

        //将环境中的t1表，转换成Table对象
        Table t1 = streamTableEnv.from("t1");

        // 4. 输出
        // 1) 表转流
        DataStream<Row> resultDs = streamTableEnv.toDataStream(resultTable);
        resultDs.print();
        // 2) 连接器表输出
        //streamTableEnv.executeSql( " create table xxx ( 字段信息 ) with ( 连接器参数 )");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

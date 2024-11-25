package com.atguigu.flink.sql.wordcount;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 步骤:
 *    1. 准备表执行环境
 *    2. 创建表
 *    3. 对表中的数据进行查询转换处理
 *    4. 输出结果
 *    5. 启动执行
 */
public class Flink02_SQL {
    public static void main(String[] args) {
        // 1. 创建表执行环境
        // 创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 基于流环境， 创建表执行环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
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
        //3. 对表中的数据进行查询转换处理
        // sql
        // 在环境中注册表
        streamTableEnv.createTemporaryView("t1", table);
        // 以下两个sql都行
        //String sql = "select id, vc, ts from "+table+" where vc >= 100";
        String sql = "select id, vc, ts from "+"t1"+" where vc >= 100";
        System.out.println(sql);
        Table resultTable = streamTableEnv.sqlQuery(sql);

        // 输出结果
        resultTable.execute().print();


        // 如果上面有execute，则以下代码不用了，如果是有界流，以下代码还会报错
        //try {
        //    env.execute();
        //} catch (Exception e) {
        //    throw new RuntimeException(e);
        //}
    }
}

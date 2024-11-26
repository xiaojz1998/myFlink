package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

//用户自定义TableFunction
//类似炸裂函数
public class Flink15_UserDefineTableFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //流表环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        // 流
        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new WaterSensor(fields[0].trim(), Integer.valueOf(fields[1].trim()), Long.valueOf(fields[2].trim()));
                        }
                );

        //流转表
        Schema schema = Schema.newBuilder()
                .column("id" , "STRING")
                .column("vc" ,"INT")
                .column("ts" , "BIGINT")
                .columnByExpression("pt" , "PROCTIME()")
                .columnByExpression("et" , "TO_TIMESTAMP_LTZ(ts, 3)")
                .watermark("et" , "et - INTERVAL '2' SECOND ")
                .build();
        Table table = streamTableEnv.fromDataStream(ds, schema);
        streamTableEnv.createTemporaryView("t1" , table );
        // 实现一个分隔字符串的函数 , 可以将一个字符串转换成（字符串，长度）的Row。
        // 输入: hello-world-nihaoa
        // 输出:
        //     word    len
        //     hello    5
        //     world    5
        //     nihaoa   6

        // 注册函数
        streamTableEnv.createTemporarySystemFunction("MySplitTableFunction" , MySplitTableFunction.class);
        // 使用函数
        // api
        table.joinLateral(call("MySplitTableFunction", $("id"),"-"))
                .select($("id"), $("word"), $("len"),$("vc"),$("ts"))
                .execute()
                .print();
        // sql
        streamTableEnv.sqlQuery(
                " select id , word , len , vc ,ts "+
                        " from t1 "+
                        " left join lateral table(MySplitTableFunction(id,'-')) on true"
        ).execute().print();
    }
    /**
     * 自定义TableFunction
     * 1. 继承 TableFunction
     * 2. 实现一个或者多个计算方法， 方法名 eval()
     */
    // 通过注释来确定返回的类型和列名
    @FunctionHint(output = @DataTypeHint("ROW<word STRING, len INT>"))
    public static class MySplitTableFunction extends TableFunction<Row>{
        public void eval(String line,String seperator) {
            String[] sl = line.split(seperator);
            for (String s : sl) {
                // 将每个单词封装成Row(word,len)
                collect(Row.of(s,s.length()));
            }
        }
    }
}

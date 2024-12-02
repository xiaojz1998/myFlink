package com.atguigu.flink.datasteamapi.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Title: Flink05_UserDefineSink
 * Create on: 2024/12/2 8:48
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 *  自定义sink
 */
public class Flink05_UserDefineSink {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> ds = env.fromSource(SourceUtil.getSource(), WatermarkStrategy.noWatermarks(), "dataGenSource");

        // 自定义sink
        ds.addSink(new MyPrintSink("ps"));

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public static class MyPrintSink extends RichSinkFunction<Event> {
        private String identifier = "PRINT" ;

        public MyPrintSink(){}

        public MyPrintSink(String identifier){
            this.identifier = identifier ;
        }
        @Override
        public void invoke(Event value, Context context) throws Exception {
            System.out.println(identifier +"> " + JSON.toJSONString( value ));
        }
    }
}

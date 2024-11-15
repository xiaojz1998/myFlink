package com.atguigu.flink.datasteamapi.source;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 用户自定义数据源
 *
 * 用 SourceFunction 实现
 *
 */
public class Flink05_UserDefineSourceFunction {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //添加数据源，用的是内部类
        //DataStreamSource<String> ds = env.addSource(new MySourceFunction());

        // 用的是自定义工具类中的数据源生成类
        DataStreamSource<Event> ds = env.fromSource(SourceUtil.getSource(), WatermarkStrategy.noWatermarks(), "MySource");


        //DataStreamSource<Event> ds = env.addSource(SourceUtil.getSourceFunction());


        ds.print();


        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 要用SourceFunction 要实现这个接口 泛型是生产数据的类型
    public static class MySourceFunction implements SourceFunction<String> {

        private Boolean isContinue = true;

        // 生成数据控制方法
        // 比如从文件、kafka、socket读数据在run中写
        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            // 每秒生成一条String 数据
            // 用isContinue 来控制循环停止
            while(isContinue){
                String data = UUID.randomUUID().toString();
                sourceContext.collect(data);

                // 休眠
                // 休眠方法
                TimeUnit.SECONDS.sleep(1);
            }
        }

        // 停止生成数据的方法
        // 想要停止调用这个方法即可
        @Override
        public void cancel() {
            isContinue = false;
        }
    }
}

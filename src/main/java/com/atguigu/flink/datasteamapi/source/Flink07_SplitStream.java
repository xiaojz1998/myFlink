package com.atguigu.flink.datasteamapi.source;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 分流
 *   1. 简单实现 ， 使用filter算子实现
 *   2. 使用侧输出流实现，推荐使用。
 */
public class Flink07_SplitStream {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> ds = env.fromSource(SourceUtil.getSource(), WatermarkStrategy.noWatermarks(), "mySource");

        // 需求: 将 Tom 分到一条流， Jerry分到一条流， 其他的分到一条流
        // 使用filter实现
        //SingleOutputStreamOperator<Event> TomDs = ds.filter(event -> "Tom".equals(event.getUser()));
        //SingleOutputStreamOperator<Event> JerryDs = ds.filter(event -> "Jerry".equals(event.getUser()));
        //SingleOutputStreamOperator<Event> otherDs = ds.filter(event -> !("Tom".equals(event.getUser()) || "Jerry".equals(event.getUser())));
        //
        //TomDs.print("TOM");
        //JerryDs.print("JERRY");
        //otherDs.print("OTHER");

        //使用 ProcessFunction 侧输出流来实现
        // 输出标签
        OutputTag<Event> tomTag = new OutputTag<Event>("tomTag", Types.POJO(Event.class));
        OutputTag<Event> jerryTag = new OutputTag<Event>("jerryTag", Types.POJO(Event.class));
        OutputTag<Event> otherTag = new OutputTag<Event>("otherTag", Types.POJO(Event.class));

        SingleOutputStreamOperator<Event> processDs = ds.process(
                new ProcessFunction<Event, Event>() {
                    // 三个参数，第一个参数是传入值，第二个是侧输出流，第三个本算子输出
                    @Override
                    public void processElement(Event event, ProcessFunction<Event, Event>.Context context, Collector<Event> collector) throws Exception {
                        if ("Tom".equals(event.getUser())) {
                            context.output(tomTag, event);
                        } else if ("Jerry".equals(event.getUser())) {
                            context.output(jerryTag, event);
                        } else {
                            // 用侧输出流输出
                            // context.output(otherTag, event);
                            // 保留在主流中
                            collector.collect(event);
                        }

                    }
                }
        );

        // 捕获侧输出流
        SideOutputDataStream<Event> TomDs = processDs.getSideOutput(tomTag);
        SideOutputDataStream<Event> JerryDs = processDs.getSideOutput(jerryTag);
        // SideOutputDataStream<Event> otherDs = processDs.getSideOutput(otherTag);


        TomDs.print("TOM");
        JerryDs.print("JERRY");
        //otherDs.print("OTHER");
        processDs.print("OTHER");


        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

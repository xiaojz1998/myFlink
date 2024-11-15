package com.atguigu.flink.datasteamapi.transformation;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 基本转换算子:
 *   map      :  映射  ， 将一个输入数据经过映射处理， 输出一个数据
 *   filter   :  过滤  ， 将一个输入数据经过过滤处理， 满足条件的输出，不满足条件的不输出
 *   flatmap  :  扁平映射 ， 将一个输入数据经过扁平映射处理， 输出0到多个数据
 *
 */
public class Flink01_BaseOperator {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> ds = env.fromSource(SourceUtil.getSource(), WatermarkStrategy.noWatermarks(), "MySource");
        // 这是对输入数据打印一个开头标记
        //ds.print("INPUT");

        // map : 将Event数据转换成Json格式的字符串输出
        // 在map下面new 一个 mapFunction（是一个接口）
        // 实现接口下的map方法
        SingleOutputStreamOperator<String> mapDs = ds.map(
                new MapFunction<Event, String>() {
                    @Override
                    public String map(Event event) throws Exception {
                        return JSON.toJSONString(event);
                    }
                }
                //JSON::toJSONString
        );

        //mapDs.print("MAP");


        //filter: 将Event中user为"Tom" 和 "Jerry"的数据过滤掉
        // 实现一个filterFunction接口
        SingleOutputStreamOperator<Event> filterDs = ds.filter(
                new FilterFunction<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return !("Tom".equals(event.getUser()) || "Jerry".equals(event.getUser()));
                    }
                }
        );

        //filterDs.print("FILTER");

        //flatmap: 将Event中user为"Tom" 和 "Jerry"的数据过滤掉 ， 同时将Event的三个属性拆分输出
        SingleOutputStreamOperator<String> flatMapDs = ds.flatMap(
                new FlatMapFunction<Event, String>() {
                    @Override
                    public void flatMap(Event event, Collector<String> collector) throws Exception {
                        if (!("Tom".equals(event.getUser()) || "Jerry".equals(event.getUser()))) {
                            collector.collect(event.getUser());
                            collector.collect(event.getUrl());
                            collector.collect(event.getTs() + "");

                        }
                    }
                }
        );

        flatMapDs.print("FLATMAP");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }
}

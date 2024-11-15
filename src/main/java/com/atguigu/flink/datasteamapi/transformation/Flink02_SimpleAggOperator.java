package com.atguigu.flink.datasteamapi.transformation;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.WordCount;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 * 简单聚合算子
 *   sum  min  minBy  max  maxBy
 *
 * 口诀: 要聚合，先keyBy .
 *      经过keyBy()后，返回的是 KeyedStream ， 可以调用聚合算子 。 普通的DataStream没有定义聚合算子
 */
public class Flink02_SimpleAggOperator {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> ds = env.fromSource(SourceUtil.getSource(), WatermarkStrategy.noWatermarks(), "MySource");

        ds.print("INPUT");
        // 统计每个user的访问次数
        SingleOutputStreamOperator<WordCount> sumDs = ds.map(event -> new WordCount(event.getUser(), 1L))
                .keyBy(WordCount::getWord)
                .sum("count");

        //sumDs.print("SUM");

        // 统计总的访问次数
        SingleOutputStreamOperator<Tuple1<Long>> sumDs1 = ds.map(event -> Tuple1.of(1L)).returns(Types.TUPLE(Types.LONG))
                .keyBy(t -> true)
                .sum(0);

        //sumDs1.print("SUM1");



        // min  minBy
        // max  maxBy ：统计每个user的最后访问的数据

        SingleOutputStreamOperator<Event> maxTs = ds.keyBy(Event::getUser).max("ts");

        //maxTs.print("MAX");

        SingleOutputStreamOperator<Event> maxByTs = ds.keyBy(Event::getUser).maxBy("ts");

        maxByTs.print("MAXBY");
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

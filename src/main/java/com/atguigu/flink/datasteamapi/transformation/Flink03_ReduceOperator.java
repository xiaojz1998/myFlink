package com.atguigu.flink.datasteamapi.transformation;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.WordCount;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *   sum += value  =>  sum = sum + value
 * 归约聚合: reduce
 *    聚合原理:  两两聚合。  每个key对应的上一次的聚合结果和本次的输入数据进行聚合。  每个key的第一个输入数据不进行聚合操作，支持输出结果。
 * ReduceFunction:
 *   一个泛型:
 *        T ：输入类型和输出类型
 *   一个方法:
 *       T reduce(T value1, T value2) throws Exception;
 *         两个参数:
 *             T value1 : 上一次的聚合结果
 *             T value2 : 本次输入的数据
 *         返回值:
 *             T: Reduce要求 输入类型 和 输出类型必须保持一致。
 */
public class Flink03_ReduceOperator {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> ds = env.fromSource(SourceUtil.getSource(), WatermarkStrategy.noWatermarks(), "Mysource");
        
        ds.print("INPUT");

        //统计每个url的点击次数
        SingleOutputStreamOperator<WordCount> reduceDs = ds.map(event -> new WordCount(event.getUrl(), 1L)).returns(Types.POJO(WordCount.class))
                .keyBy(WordCount::getWord)
                .reduce(
                        new ReduceFunction<WordCount>() {
                            @Override
                            public WordCount reduce(WordCount wordCount, WordCount t1) throws Exception {
                                System.out.println("reduce....");
                                return new WordCount(wordCount.getWord(), wordCount.getCount() + t1.getCount());
                            }
                        }
                );

        reduceDs.print("结果是");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }
}

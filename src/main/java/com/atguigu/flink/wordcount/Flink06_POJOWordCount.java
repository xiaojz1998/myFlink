package com.atguigu.flink.wordcount;

import com.atguigu.flink.pojo.WordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *
 * WordCount - 使用POJO封装数据 - DataStream
 * 步骤:
 *   1. 创建执行环境
 *   2. 从数据源读取数据
 *   3. 对读取到的数据进行转换处理
 *   4. 写出结果
 *   5. 启动执行
 */
public class Flink06_POJOWordCount {
    public static void main(String[] args) {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(1);

        // 2. 从数据源读取数据
        // DataStreamSource extends DataStream
        // 从端口读取数据
        DataStreamSource<String> ds = env.socketTextStream("hadoop102", 8888);

        // 3.对读取到的数据进行转换处理
        // 3.1 切分单词，再将切分出来的每个单词处理成(word,1)的格式
        // SingleOutputStreamOperator extends DataStream
        SingleOutputStreamOperator<WordCount> flatMapDs = ds.flatMap(
                new FlatMapFunction<String, WordCount>() {
                    @Override
                    public void flatMap(String s, Collector<WordCount> collector) throws Exception {
                        String[] s1 = s.split(" ");
                        for (String word : s1) {
                            collector.collect(new WordCount(word, 1L));
                        }
                    }
                }
        );

        // 3.2 按照 单词 进行分组
        /*
              KeySelector:
                 两个泛型:
                     <IN> – Type of objects to extract the key from.  输入数据类型
                     <KEY> – Type of key.  提取到的key的类型
                 一个方法:
                     KEY getKey(IN value) throws Exception;
                        参数:
                           IN value : 输入数据
                        返回值:
                           KEY : 提取到的key
         */
        // KeyedStream extends DataStream
        KeyedStream<WordCount, String> keyByDs = flatMapDs.keyBy(
                new KeySelector<WordCount, String>() {
                    @Override
                    public String getKey(WordCount wordCount) throws Exception {
                        return wordCount.getWord();
                    }
                }
        );

        //3.3 统计每个单词出现的次数
        /*
                sum(int) :  如果当前数据类型是Tuple ，指定使用Tuple的第几个元素进行累加求和
                sum(String):如果当前数据类型是POJO ，指定使用POJO的哪个属性进行累加求和
         */
        SingleOutputStreamOperator<WordCount> sumDs = keyByDs.sum("count");

        // 4. 写出结果 用的bean/pojo 的 toString方法
        sumDs.print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

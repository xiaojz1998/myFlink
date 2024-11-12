package com.atguigu.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * wordcount 有界流处理
 * 步骤
 * 1. 创建执行环境
 * 2. 从文件中读取数据
 * 3. 分组、聚合等中间处理
 * 4. 写入文件/打印
 * 5. 启动执行
 */
public class Flink02_BoundedStreamWordCount {
    public static void main(String[] args) {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度为1
        env.setParallelism(1) ;

        // 2. 从文件中读取数据
        // DataStreamSource 继承自 DataStream
        String inputPath = "input/word.txt";
        DataStreamSource<String> ds = env.readTextFile(inputPath);

        // 3. 分组、聚合等中间处理
        // 3.1 切分单词，再将切分出来的每个单词处理成(word,1)的格式
        // SingleOutputStreamOperator extends DataStream
        SingleOutputStreamOperator<Tuple2<String, Long>> flatMapDs = ds.flatMap(
                new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        String[] s1 = s.split(" ");
                        for (String word : s1) {
                            collector.collect(Tuple2.of(word, 1L));
                        }
                    }
                }
        );
        // 3.2 按照 单词 进行分组
        // 用的方法和DataSet不同 DataSet是groupBy
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
        KeyedStream<Tuple2<String, Long>, String> keyByDs = flatMapDs.keyBy(
                new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> stringLongTuple2) throws Exception {
                        return stringLongTuple2.f0; // 使用Tuple的第一个元素作为key
                    }
                }
        );

        //3.3 统计每个单词出现的次数
        /*
                sum(int) :  如果当前数据类型是Tuple ，指定使用Tuple的第几个元素进行累加求和
                sum(String):如果当前数据类型是POJO ，指定使用POJO的哪个属性进行累加求和
         */
        SingleOutputStreamOperator<Tuple2<String, Long>> sumDs = keyByDs.sum(1); // 使用Tuple的第二个元素进行累加求和

        // 4. 写出结果
        sumDs.print();

        // 5. 启动执行
        // 注意用的是env
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}

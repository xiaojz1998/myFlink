package com.atguigu.flink.wordcount;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *
 * WordCount - 流批一体 - DataStream
 * 步骤:
 *   1. 创建执行环境
 *   2. 从数据源读取数据
 *   3. 对读取到的数据进行转换处理
 *   4. 写出结果
 *   5. 启动执行
 *
 * 执行模式:
 *     RuntimeExecutionMode:
 *        STREAMING: 流处理（默认情况） , 有界和无界流都可以进行流处理。
 *        BATCH :    批处理 , 有界流可以进行批处理，无界流不能进行批处理。
 *        AUTOMATIC : 自动选择。 如果是有界流， 执行批模式， 如果是无界流，执行流模式。
 *
 * 设置执行模式:
 *     1. 代码中设置 （不推荐）
 *         env.setRuntimeMode( RuntimeExecutionMode.AUTOMATIC ) ;
 *
 *     2. 在提交作业的时候，通过参数来指定执行模式
 *        flink run  -Dexecution.runtime-mode=BATCH|STREAMING|AUTOMATIC  ...
 *
 */
public class Flink04_StreamBatchWordCount {
    public static void main(String[] args) {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(1);

        // 设置运行模式
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // 2. 从数据源读取数据
        // DataStreamSource extends DataStream
        // 从端口读取数据
        DataStreamSource<String> ds = env.readTextFile("input/word.txt");
                                        //env.socketTextStream("hadoop102" , 8888) ;

        // 3.对读取到的数据进行转换处理
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
                        return stringLongTuple2.f0;
                    }
                }
        );
        //3.3 统计每个单词出现的次数
        /*
                sum(int) :  如果当前数据类型是Tuple ，指定使用Tuple的第几个元素进行累加求和
                sum(String):如果当前数据类型是POJO ，指定使用POJO的哪个属性进行累加求和
         */
        SingleOutputStreamOperator<Tuple2<String, Long>> sumDs = keyByDs.sum(1);

        sumDs.print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }
}

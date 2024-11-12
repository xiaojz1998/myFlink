package com.atguigu.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * WordCount - 函数实现方式 - DataStream
 * 步骤:
 *   1. 创建执行环境
 *   2. 从数据源读取数据
 *   3. 对读取到的数据进行转换处理
 *   4. 写出结果
 *   5. 启动执行
 * 函数的实现方式:
 *   1. 自定义函数类，实现函数接口
 *   2. 使用匿名内部类的方式实现函数接口
 *   3. Lambda表达式的方式实现函数接口
 *      注意泛型擦除问题: The generic type parameters of 'Collector' are missing.
 *      解决办法: You can give type information hints by using the returns(...) method on the result of the transformation call
 */
public class Flink05_FunctionImplementWordCount {
    public static void main(String[] args) {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度1
        env.setParallelism(1);

        // 2. 从数据源读取数据
        // DataStreamSource extends DataStream
        // 从端口读取数据
        DataStreamSource<String> ds = env.socketTextStream("hadoop102", 8888);

        // 3.对读取到的数据进行转换处理
        // 3.1 切分单词，再将切分出来的每个单词处理成(word,1)的格式
        // SingleOutputStreamOperator extends DataStream
        SingleOutputStreamOperator<Tuple2<String, Long>> flatMapDs = ds.flatMap(
                //1. 自定义函数类，实现函数接口
                // new myFlatMapFunction(" ")

                //2.使用匿名内部类的方式实现函数接口
                //new FlatMapFunction<String, Tuple2<String, Long>>() {
                //    @Override
                //    public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
                //        String[] words = line.split(" ");
                //        for (String word : words) {
                //            out.collect(Tuple2.of(word, 1L));
                //        }
                //    }
                //}

                // 3. Lambda表达式的方式实现函数接口
                // 注意括号里面的只是形参，程序不能根据他来推断参数类型
                (String line, Collector<Tuple2<String, Long>> out)->{
                        String[] words = line.split(" ");
                        for (String word : words) {
                            out.collect(Tuple2.of(word, 1L));
                        }
                }
        ).returns(
                //1. XXX.class

                //2. TypeHits
                //new TypeHint<Tuple2<String, Long>>() {
                //    @Override
                //    public TypeInformation<Tuple2<String, Long>> getTypeInfo() {
                //        return super.getTypeInfo();
                //    }
                //}

                //3. TypeInformation
                Types.TUPLE(Types.STRING, Types.LONG)
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
                //new KeySelector<Tuple2<String, Long>, String>() {
                //    @Override
                //    public String getKey(Tuple2<String, Long> stringLongTuple2) throws Exception {
                //        return stringLongTuple2.f0;
                //    }
                //}
                // 用lambda表达式
                v -> v.f0
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
    public static class myFlatMapFunction implements FlatMapFunction<String, Tuple2<String,Long>>{

        private String seperator ;

        public myFlatMapFunction(String seperator) {
            this.seperator = seperator;
        }

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
            String[] s1 = s.split(seperator);
            for (String word : s1) {
                collector.collect(Tuple2.of(word,1L));
            }
        }
    }
}

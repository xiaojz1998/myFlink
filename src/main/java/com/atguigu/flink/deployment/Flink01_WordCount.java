package com.atguigu.flink.deployment;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink01_WordCount {
    public static void main(String[] args) {

        System.out.println("hello world");
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度为1
        env.setParallelism(1);
        // 2. 从数据源读取数据
        // DataStreamSource extends DataStream
        // 从端口读取数据

        // 获取外部传入的参数
        // 通过下标来获取
        //String host = args[0] ;
        //int port = Integer.parseInt(args[1]);

        // 通过参数名来获取
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("hostname");
        int port = parameterTool.getInt("port");

        DataStreamSource<String> ds = env.socketTextStream(host, port);

        // 3.对读取到的数据进行转换处理
        // 3.1 切分单词，再将切分出来的每个单词处理成(word,1)的格式

        // SingleOutputStreamOperator extends DataStream
        SingleOutputStreamOperator<Tuple2<String, Long>> flatMapDs = ds.flatMap(
                new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] words = line.split(" ");
                        for (String word : words) {
                            out.collect(Tuple2.of(word, 1L));
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
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;  // 使用Tuple的第一个元素作为key
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
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}

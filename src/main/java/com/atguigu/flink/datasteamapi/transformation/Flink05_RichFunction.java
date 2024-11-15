package com.atguigu.flink.datasteamapi.transformation;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 函数类:
 *   1. 普通函数类 , 只关注函数本身对数据处理的功能
 *
 *          算子              传入的函数           函数的方法
 *         map()     ->    MapFunction         ->  map()
 *         filter()  ->    FilterFunction      ->  filter()
 *         flatmap() ->    FlatMapFunction     ->  flatMap()
 *         reduce()  ->    ReduceFunction      ->  reduce
 *         ......
 *
 *   2. 富函数类
 *         RichFunction  接口
 *            AbstractRichFunction 抽象类实现了默认的方法
 *                  算子              传入的函数              函数的方法
 *                  map()     ->    RichMapFunction         ->  map()
 *                  filter()  ->    RichFilterFunction      ->  filter()
 *                  flatmap() ->    RichFlatMapFunction     ->  flatMap()
 *                  reduce()  ->    RichReduceFunction      ->  reduce
 *                  ......
 *
 *   3. 富函数类的功能:
 *        1)  对数据处理的功能，
 *
 *        2)  生命周期方法 , 算子的每个并行实例
 *            注意 close 只有在有界流才会调用
 *               open()    算子的每个并行实例创建的时候调用一次 (在数据处理之前) 用处 eg：初始化连接池（资源）
 *               xxxx()    对数据进行处理的方法 看你用的什么富函数
 *               close()   算子的每个必须宁实例销毁的时候调用一次(在数据处理之后) 用处 eg：关闭连接池（资源）
 *
 *        3)  运行时上下文对象 RuntimeContext ， 可以获取各种信息 及 获取按键分区状态进行状态编程
 *              有很多方法，这里只介绍重点的
 *             状态编程:
 *                 ①  getState()
 *                 ②  getListState()
 *                 ③  getMapState()
 *                 ④  getReducingState()
 *                 ⑤  getAggregatingState()
 *
 */
public class Flink05_RichFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Stream 有界流
        FileSource<String> fileSource = FileSource.<String>forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path("input/event.txt")
        ).build();

        SingleOutputStreamOperator<Event> mapDs = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "fileSource")
                .map(
                        line -> {
                            String[] split = line.split(",");
                            return new Event(split[0].trim(), split[1].trim(), Long.valueOf(split[2]));
                        }
                );

        mapDs.print("INPUT");
        // 将输入数据转换成Json格式的字符串输出
        SingleOutputStreamOperator<String> mapDs1 = mapDs.map(
                // 富函数类
                new RichMapFunction<Event, String>() {
                    /**
                     * 在算子的每个并行实例创建时调用一次， 数据处理之前。
                     */
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("获取Jedis对象.....");
                    }

                    @Override
                    public String map(Event event) throws Exception {
                        // 假定： 处理每条数据的时候，需要访问外部存储组件中的数据（例如Redis）
                        // Jedis / Lettuce
                        System.out.println("访问Redis,读写数据.....");
                        return JSON.toJSONString(event);
                    }

                    /**
                     * 在算子的每个并行实例销毁时调用一次， 数据处理之后
                     */
                    @Override
                    public void close() throws Exception {
                        System.out.println("释放Jedis对象.....");
                    }
                }
        );

        mapDs1.print("OUTPUT");


        // 运行部分
        try {
            env.execute();
        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }
}

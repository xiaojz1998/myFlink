package com.atguigu.flink.datasteamapi.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class Flink01_SimpleSource {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1. 从集合中读取数据
        List<String> dataList = Arrays.asList("hello", "world", "flink", "spark", "hadoop");
        DataStreamSource<String> ds1 = env.fromCollection(dataList);
        ds1.print("ds1"); // 可以在print中加一个标记，即每次打印在开头

        DataStreamSource<String> ds2 = env.fromElements("flume", "kafka", "hive", "zookeeper", "java");
        ds2.print("ds2");

        //2. 从端口读取数据
        //DataStreamSource<String> ds3 = env.socketTextStream("hadoop102", 8888);
        //ds3.print("DS3");

        //3. 从文件读取数据
        DataStreamSource<String> ds4 = env.readTextFile("input/word.txt");
        ds4.print("ds4");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}

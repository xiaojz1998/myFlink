package com.atguigu.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/** WordCount - 批处理 - DataSet
* 步骤:
*    1. 创建执行环境
*    2. 从数据源读取数据
*    3. 对读取到的数据进行转换处理
*    4. 写出结果
*
*/
public class Flink01_BatchWordCount {
    public static void main(String[] args) {
        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 从数据源中读取数据
        // DataSource extends DataSet
        // 相对路径
        DataSource<String> ds = env.readTextFile("input/word.txt");
        // 绝对路径
        //DataSource<String> ds = env.readTextFile("D:\\peixunban\\code\\flink_code\\myFlink\\input\\word.txt");

        // 3. 对读取到的数据进行转换处理
        // 3.1 切分单词，再将切分出来的每个单词处理成(word,1)的格式
        /*
           FlatMapFunction:
              两个泛型:
                 <T> – Type of the input elements.    输入数据类型
                 <O> – Type of the returned elements. 输出数据类型

              一个方法:
                 void flatMap(T value, Collector<O> out) throws Exception;
                    两个参数:
                        T value :   输入数据
                        Collector<O> out :  输出数据收集器
         */
        // FlatMapOperator extends DataSet
        FlatMapOperator<String, Tuple2<String, Long>> flatMapDs = ds.flatMap(
                new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        String[] split = s.split(" ");
                        for (String word : split) {
                            collector.collect(Tuple2.of(word, 1L));
                        }
                    }
                }
        );

        // 3.2 按照 单词 进行分组
        /*
            groupBy(int):   如果当前的数据类型是Tuple类型，通过数字指定Tuple中的第几个元素作为分组的key
            groupBy(String) : 如果当前的数据类型是POJO类型（简单理解为JavaBean），指定使用对象的哪个属性作为分组的key
         */
        UnsortedGrouping<Tuple2<String, Long>> groupByDs = flatMapDs.groupBy(0);

        //3.3 统计每个单词出现的次数
        // AggregateOperator extends DataSet
        AggregateOperator<Tuple2<String, Long>> sumDs = groupByDs.sum(1);

        // 4.  写出结果
        try {
            sumDs.print();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

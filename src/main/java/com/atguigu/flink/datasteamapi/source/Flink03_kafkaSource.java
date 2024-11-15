package com.atguigu.flink.datasteamapi.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;

/**
 * Kafka Connector :
 *   Kafka Source
 *
 * Kafka Consumer:
 *   1. 消费者对象 ：KafkaConsumer
 *   2. 创建消费者对象
 *      KafkaConsumer consumer = new KafkaConsumer( properties );
 *   3. 消费者配置
 *      1） Kafka集群位置
 *           bootstrap.servers
 *      2） 消费者组
 *           group.id
 *      3） key和value的反序列化器
 *           key.deserializer
 *           value.deserializer
 *      4） offset自动提交     offset[ groupId - topic - partition - offset ]
 *          enable.auto.commit
 *          auto.commit.interval.ms
 *      5） offset重置
 *            auto.offset.reset
 *               发生重置的情况:
 *                  What to do when there is no initial offset in Kafka
 *                  or
 *                  if the current offset does not exist any more on the server (e.g. because that data has been deleted):
 *               重置策略:
 *                  earliest: automatically reset the offset to the earliest offset 最早的位置，可以消费到分区中已有的数据
 *                  latest: automatically reset the offset to the latest offset  最新的位置，只能消费新到的数据
 *      6） 事务隔离级别
 *           isolation.level
 *              read_committed    读已提交
 *              read_uncommitted  读未提交
 *   3. 消费者存在的问题
 *          重复消费
 *          漏消费
 */
public class Flink03_kafkaSource {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 指定offset
        // TopicPartition是kafka提供的类，用于存储话题和偏移量
        HashMap<TopicPartition,Long> offsets = new HashMap<>();
        //offsets.put(new TopicPartition("first",0),10L);

        // kafkaSource
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setGroupId("flink")
                .setTopics("first")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(
                        //OffsetsInitializer.earliest()
                        OffsetsInitializer.latest()
                )
                //其他的参数，统一使用setProperty方法进行设置
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .build();

        DataStreamSource<String> ds = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");

        ds.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

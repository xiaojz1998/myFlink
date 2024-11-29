package com.atguigu.flink.checkpoint;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;


/**
 * Kafka(输入端) -> Flink -> Kafka(输出端)
 */
public class Flink02_KafkaToFlinkToKafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 如果不开检查点，则只能读取未commit数据
        env.enableCheckpointing(2000l);
        // kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setGroupId("myflink")
                .setTopics("flink")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(
                        //OffsetsInitializer.earliest()
                        //OffsetsInitializer.latest()
                        OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST)
                        //OffsetsInitializer.offsets(offsets)
                )
                //其他的参数，统一使用setProperty方法进行设置
                //Flink 只会消费那些已经被提交的事务消息，以及非事务性的消息。
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .build();

        DataStreamSource<String> ds = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");

        // Flink处理
        // 省略
        // KafkaSink
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("topicG")
                                .setValueSerializationSchema(
                                        new SimpleStringSchema()
                                )
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG , "600000")
                .setTransactionalIdPrefix("flink-"+ System.currentTimeMillis())
                .build();

        ds.sinkTo( kafkaSink );
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

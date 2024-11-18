package com.atguigu.flink.datasteamapi.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * KafkaConnector
 *    KafkaSink
 * KafkaProducer:
 *   1. 创建生产者对象
 *      KafkaProducer producer = new KafkaProducer( properties );
 *   2. 生产者配置
 *      1) 集群位置
 *          bootstrap.servers
 *      2) key 和 value的序列化器
 *           key.serializer
 *           value.serializer
 *      3) 分区
 *          partitioner.class  , 默认使用 org.apache.kafka.clients.producer.internals.DefaultPartitioner
 *          If a partition is specified in the record, use it
 *          如果明确指定分区号， 直接使用
 *          If no partition is specified but a key is present choose a partition based on a hash of the key
 *          如果没有指定分区号，但是有key ,根据key的hash值对分区数取模计算分区号
 *          If no partition or key is present choose the sticky partition that changes when the batch is full.
 *          如果没有指定分区号，也没有key, 使用黏性策略。
 *      4) 应答级别
 *          acks = 0,1,-1（all）
 *      5) 事务超时时间
 *         transaction.timeout.ms
 *      6) 事务Id
 *         transactional.id
 *
 *   3.KafkaSink  DeliveryGuarantee.EXACTLY_ONCE 注意事项:
 *      1) 报错：The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).
 *         Kafka集群限制的事务的最大超时时间: transaction.max.timeout.ms = 900000 (15 minutes) 是15分钟
 *         生产者事务超时时间: transaction.timeout.ms = 60000 (1 minute) 这是kafka内置的而不是flink的
 *         Flink KakfaSink默认的生产者事务超时时间 DEFAULT_KAFKA_TRANSACTION_TIMEOUT = Duration.ofHours(1); 一个小时，所以超时了报错
 *         解决：.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG , "600000")
 */
public class Flink02_KafkaSink {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 开启检查点
        env.enableCheckpointing(3000L);

        DataStreamSource<Event> ds = env.fromSource(SourceUtil.getSource(), WatermarkStrategy.noWatermarks(), "mySource");
        SingleOutputStreamOperator<String> mapDs = ds.map(JSON::toJSONString);

        // 把数据写入到kafka主题中
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                // 配置kv序列化
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("flink")
                                .setValueSerializationSchema(
                                        new SimpleStringSchema()
                                )
                                .build()
                )
                // 配置应答级别 默认是AT_LEAST_ONCE
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                //.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 配置生产者事务超时时间 在应答级别是exactly_once的时候，必须配置，否则报错
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "600000")
                .setTransactionalIdPrefix("flink-" + System.currentTimeMillis())
                .build();
        mapDs.sinkTo(kafkaSink);
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

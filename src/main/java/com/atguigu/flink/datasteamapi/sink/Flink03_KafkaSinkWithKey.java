package com.atguigu.flink.datasteamapi.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class Flink03_KafkaSinkWithKey {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 开启检查点
        //env.enableCheckpointing(2000l);
        DataStreamSource<Event> ds = env.fromSource(SourceUtil.getSource(), WatermarkStrategy.noWatermarks(), "dataGenSource");

        // 把数据写道kafka flink主题中
        KafkaSink<Event> kafkaSink = KafkaSink.<Event>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setRecordSerializer(
                        //自定义序列化 ，实现带key写入
                        new KafkaRecordSerializationSchema<Event>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(Event event, KafkaSinkContext kafkaSinkContext, Long aLong) {
                                byte[] key = event.getUser().getBytes() ;
                                byte [] value = JSON.toJSONString(event).getBytes();

                                ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>("flink", key, value);
                                return producerRecord;

                            }
                        }
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        ds.sinkTo(kafkaSink);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}

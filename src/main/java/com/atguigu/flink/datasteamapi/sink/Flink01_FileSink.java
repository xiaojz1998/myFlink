package com.atguigu.flink.datasteamapi.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

/**
 *
 * Filesystem connector
 *  FileSink
 *
 *  注意要开检查点 nv.enableCheckpointing(2000L);
 *
 *  两种sink：
 *   1. SinkFunction   -> addSink()
 *   2. Sink           -> sinkTo()
 *  对应两种source
 *  1. SourceFunction  ->  addSource()
 *  2. Source          -> fromSource()
 */
public class Flink01_FileSink {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // 开启检查点
        // 如果不开启检查点文件名会非常长，且没有关闭，所以不建议使用
        // 只有开启检查点才能真正完成写入
        env.enableCheckpointing(2000L);

        DataStreamSource<Event> ds = env.fromSource(SourceUtil.getSource(), WatermarkStrategy.noWatermarks(), "MySource");
        SingleOutputStreamOperator<String> mapDs = ds.map(event -> JSON.toJSONString(event));

        // 将数据写入到文件中
        // 这里先用build才能.var
        // 在build之前要设置参数
        FileSink<String> fileSink = FileSink.<String>forRowFormat(
                new Path("output"),
                new SimpleStringEncoder<>("UTF-8")
        )
                .withRollingPolicy( // 文件滚动策略
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(new MemorySize(1024*1024*10)) // 滚动大小
                                // .withMaxPartSize(MemorySize.parse("10MB"))        // 此函数自动识别大小 功能与上个函数相同
                                .withRolloverInterval(Duration.ofSeconds(10))        // 十秒滚动间隔
                                .withInactivityInterval(Duration.ofSeconds(5))       // 非活跃间隔多少秒滚动一次
                                .build()
                )
                // 目录滚动策略
                .withBucketAssigner(
                        new DateTimeBucketAssigner<>("yyyy-MM-dd HH-mm") // 没分钟滚动，且命名，用-是因为目录中不允许用:
                )
                .withBucketCheckInterval(1000l) // 目录滚动 检测时间间隔
                .withOutputFileConfig(          // 文件名策略
                        OutputFileConfig.builder()
                                .withPartPrefix("flink")  // 文件名前缀
                                .withPartSuffix(".txt")   // 文件名后缀
                                .build()
                )
                .build();

        mapDs.sinkTo(fileSink);


        try {
            env.execute();
        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }
}

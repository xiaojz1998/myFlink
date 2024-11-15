package com.atguigu.flink.datasteamapi.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * FileSystem 连接器
 *   FileSource
 * Source API:   fromSource()
 * SourceFunction API:  addSource()
 */
public class Flink02_FileSource {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // FileSource
        // 当静态方法需要泛型时候放到 . 后面
        FileSource<String> fileSource = FileSource.<String>forRecordStreamFormat(
                new TextLineInputFormat(),                  // 文本格式 一行
                new Path("input/word.txt")         // 路径
        ).build();

        // 传入filesource，水位，和一个名字
        DataStreamSource<String> ds = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "fileSource");

        ds.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

package com.atguigu.flink.checkpoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * 检查点配置
 */
public class Flink01_CheckPointConfig {
    public static void main(String[] args) {
        //设置HDFS的用户
        System.setProperty("HADOOP_USER_NAME" ,"atguigu");

        Configuration conf = new Configuration();
        conf.setInteger("rest.port" , 5678);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment( conf );
        env.setParallelism(1);

        //设置状态后端
        env.setStateBackend(new HashMapStateBackend()) ;

        // 开启检查点 , 默认是精准一次
        env.enableCheckpointing(5000L , CheckpointingMode.EXACTLY_ONCE) ;

        // 检查点配置
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        // 检查点存储位置
        // JobManagerCheckpointStorage , 将检查点存储到JobManager的内存中
        // FileSystemCheckpointStorage , 将检查点存储到指定的文件系统中 ， 例如HDFS
        checkpointConfig.setCheckpointStorage(
                new FileSystemCheckpointStorage("hdfs://hadoop102:8020/flink-ck")
        );

        // 检查点的间隔
        //checkpointConfig.setCheckpointInterval(3000L);

        // 检查点模式
        //checkpointConfig.setCheckpointingMode( CheckpointingMode.AT_LEAST_ONCE );

        // 检查点超时时间
        checkpointConfig.setCheckpointTimeout(10000L);

        // 同时执行的检查点个数
        checkpointConfig.setMaxConcurrentCheckpoints( 1 );

        // 两个检查点之间的最小间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(3000L);

        // 作业取消后，是否保留检查点
        // CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION  保留
        // CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION  删除
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        // 检查点最大失败的个数
        checkpointConfig.setTolerableCheckpointFailureNumber(3);

        // 允许开始非对齐
        checkpointConfig.enableUnalignedCheckpoints();
        // Barrier对齐的超时时间
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(5));

        // 强制开启非对齐
        checkpointConfig.setForceUnalignedCheckpoints(true);


        env.socketTextStream("hadoop102" , 8888).print() ;



        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

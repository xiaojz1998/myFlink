package com.atguigu.flink.state;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 状态后端
 *    HashMapStateBackend
 *    EmbeddedRocksDBStateBackend
 *
 * 设置状态后端:
 *    1. 通过代码指定
 *       env.setStateBackend( new HashMapStateBackend() );
 *       env.setStateBackend( new EmbeddedRocksDBStateBackend()) ;
 *       env.setStateBackend( new EmbeddedRocksDBStateBackend(true));  增量式检查点
 *
 *    2. 通过集群的flink-conf.yaml文件来配置
 *       state.backend.type: hashmap | rocksdb
 *
 *    3. 提交作业的时候，通过参数来指定
 *       bin/flink  run  -Dstate.backend=hashmap | rocksdb
 *
 */
public class Flink14_StateBackend {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port" , 5678);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        //设置使用状态后端
        // hashmap
        //env.setStateBackend( new HashMapStateBackend() );

        // rockdb
        //env.setStateBackend( new EmbeddedRocksDBStateBackend()) ;

        // 增量式检查点
        //env.setStateBackend( new EmbeddedRocksDBStateBackend(true));

        //获取当前使用的状态后端
        StateBackend stateBackend = env.getStateBackend();
        System.out.println("当前使用的状态后端: " + stateBackend );

        env.socketTextStream("hadoop102" , 8888).print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

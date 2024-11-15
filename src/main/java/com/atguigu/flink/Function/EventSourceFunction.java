package com.atguigu.flink.Function;

import com.atguigu.flink.pojo.Event;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.TimeUtils;

import java.util.concurrent.TimeUnit;

/**
 * 用SourceFunction来生成数据
 */
public class EventSourceFunction implements SourceFunction<Event> {
    String [] users = {"Zhangsan" , "Lisi" , "Wangwu" , "Tom" , "Jerry"};

    String [] urls = {"/home" , "/list" , "/detail" , "/pay", "/cart"} ;
    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        while(true){
            String user = users[RandomUtils.nextInt(0 , users.length)] ;
            String url = urls[RandomUtils.nextInt(0 ,urls.length)];
            Long ts = System.currentTimeMillis() ;

            sourceContext.collect(new Event(user , url , ts));

            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {

    }
}

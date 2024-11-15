package com.atguigu.flink.util;

import com.atguigu.flink.Function.EventSourceFunction;
import com.atguigu.flink.pojo.Event;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 生成数据的工具类
 */
public class SourceUtil {
    /**
     * 使用DataGenSource来模拟生成数据
     */

    public static DataGeneratorSource<Event> getSource(){
        return new DataGeneratorSource<Event>(
                new GeneratorFunction<Long, Event>() {
                    // 存储用于随机的用户
                    String [] users = {"Zhangsan" , "Lisi" , "Wangwu" , "Tom" , "Jerry"};
                    // 存储用于随机的url
                    String [] urls = {"/home" , "/list" , "/detail" , "/pay", "/cart"} ;

                    // 用randomutils来生成随机数
                    @Override
                    public Event map(Long aLong) throws Exception {
                        String user = users[RandomUtils.nextInt(0, users.length)];
                        String url = urls[RandomUtils.nextInt(0, urls.length)];
                        Long ts = System.currentTimeMillis();
                        return new Event(user, url, ts);
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(1),
                // 如何只能自定义类的类型
                Types.POJO(Event.class)
        );
    }

    /**
     * 使用自定义SourceFunction来模拟生成数据
     */

    public static SourceFunction<Event> getSourceFunction(){

        return new EventSourceFunction();
    }

}

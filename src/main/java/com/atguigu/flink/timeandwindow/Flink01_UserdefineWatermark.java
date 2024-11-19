package com.atguigu.flink.timeandwindow;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.stream.Stream;

/**
 *
 * 自定义水位线生成
 * 生成水位线的时机 ： Flink支持在任意算子后面去生成水位线。 一般建议在Source算子后面生成水位线， 下游的算子都可以感知时间。
 *
 * WatermarkStrategy:
 *    public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context){} ，创建 TimestampAssigner 对象
 *       TimestampAssigner：
 *          long extractTimestamp(T element, long recordTimestamp); 从数据中提取时间
 *
 *    public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context){}， 创建 WatermarkGenerator 对象
 *       WatermarkGenerator：
 *          void onEvent(T event, long eventTimestamp, WatermarkOutput output); 给每条数据生成水位线，每条数据到达后都会调用一次该方法
 *          void onPeriodicEmit(WatermarkOutput output); 周期性生成水位线， 按照指定的周期调用。
 */
public class Flink01_UserdefineWatermark {
    public static void main(String[] args)
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置周期水位线发射周期
        env.getConfig().setAutoWatermarkInterval(500);

        //DataStreamSource<Event> ds = env.fromSource(SourceUtil.getSource(), WatermarkStrategy.noWatermarks(), "MySource");

        // 使用端口读取数据
        // zhangsan,/home,1000
        SingleOutputStreamOperator<Event> ds = env.socketTextStream("hadoop102", 8888)
                .map((s) -> {
                    String[] split = s.split(",");
                    return new Event(split[0], split[1], Long.parseLong(split[2]));
                });
        ds.print("INPUT");

        /// 生成时间戳和水位线的方法
        SingleOutputStreamOperator<Event> timestampsAndWatermarks = ds.assignTimestampsAndWatermarks(
                // 有序流水位线生成
                //new OrderedWatermarkStrategy()
                // 乱序
                //new UnOrderedWatermarkStrategy()
                // 有序+乱序 一站解决
                new MyWaterMarkStrategy(2000)

        );
        timestampsAndWatermarks.print("TAW");


        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    /**
     * 有序 + 乱序水位线生成
     */
    public static class MyWaterMarkStrategy implements WatermarkStrategy<Event>{

        private long delay;
        // 传入延迟时间的构造函数
        public MyWaterMarkStrategy(long delay) {
            this.delay = delay;
        }
        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new MyWatermarkGenerator(delay);
        }

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new MyTimestampAssigner();
        }
    }
    /**
     * 有序 + 乱序水位线生成器
     */
    public static class MyWatermarkGenerator implements WatermarkGenerator<Event>{
        private long maxTs;
        private long delay;
        public MyWatermarkGenerator(long delay) {
            this.delay = delay;
            maxTs = Long.MIN_VALUE+delay;
        }
        @Override
        public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
            // 只用记录最大时间即可
            maxTs = Math.max(maxTs, l);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            Watermark watermark = new Watermark(maxTs - delay);
            watermarkOutput.emitWatermark(watermark);
            System.out.println("周期时间是"+watermark);
        }
    }
    /**
     * 有序 + 乱序时间戳捕获器
     */
    public static class MyTimestampAssigner implements TimestampAssigner<Event>{
        @Override
        public long extractTimestamp(Event event, long l) {
            return event.getTs();
        }
    }


    /**
     *  乱序水位线生成
     */
    public static class UnOrderedWatermarkStrategy implements WatermarkStrategy<Event>{

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new UnOrderedWatermarkGenerator();
        }

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new UnOrderedTimestampAssigner();
        }
    }
    /**
     * 乱序流水位线生成器 用于上方函数返回
     */
    public static class UnOrderedWatermarkGenerator implements WatermarkGenerator<Event>{

        private Long delay = 2000L; // 两秒延迟
        private long maxTs = Long.MIN_VALUE+delay;
        /**
         * 给每条数据生成水位线， 每条数据到达都会调用一次该方法
         * @param event
         * @param l
         * @param watermarkOutput
         */
        @Override
        public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
            // 原则: 以已经到达的数据中最大的时间来生成水位线
            //      如果考虑迟到数据，需要在生成水位线的时候减去延迟的时间

            // 记录最大时间
            maxTs = Math.max(maxTs, l);
            // 生成水位线
            //Watermark watermark = new Watermark(maxTs);
            //watermarkOutput.emitWatermark(watermark);
            //System.out.println("maxTs:" + maxTs);
        }

        /**
         * 周期性生成流水线
         * @param watermarkOutput
         */
        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            // 原则: 以已经到达的数据中最大的时间来生成水位线
            //      如果考虑迟到数据，需要在生成水位线的时候减去延迟的时间
            // 生成水位线
            Watermark watermark = new Watermark(maxTs-delay);
            watermarkOutput.emitWatermark(watermark);
            System.out.println("onPeriodicEmit ==> 乱序流周期性生成水位线: " + watermark );

        }
    }
    /**
     * 乱序流时间戳分配器 用于上方函数返回
     */
    public static  class UnOrderedTimestampAssigner  implements  TimestampAssigner<Event>{

        @Override
        public long extractTimestamp(Event event, long l) {
            return event.getTs();
        }
    }

    /**
    *   有序流水位线生成
    */
    public static class OrderedWatermarkStrategy implements WatermarkStrategy<Event>{
        /**
         *
         * @param context
         * @return
         */
        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new OrderedWatermarkGenerator();
        }

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new OrderedTimestampAssigner();
        }
    }
    /**
     * 有序流时间戳分配器 用于上方函数返回
     */
    public static class OrderedTimestampAssigner implements TimestampAssigner<Event>{
        /**
         * 从数据中提取时间
         * @param event 传入的数据单位
         * @param l 上一个元素的时间戳。
         *          这个参数对于某些特定的用例可能是有用的，
         *          比如当您需要基于前一个事件的时间来计算当前事件的时间戳，或者是在某些情况下用来验证时间戳的一致性等。
         *          但是，在大多数情况下，这个参数可能不会被直接使用，而是主要关注于从 element 中提取时间戳。
         * @return
         */
        @Override
        public long extractTimestamp(Event event, long l) {
            return event.getTs();
        }
    }
    /**
     * 有序流水位线生成 用于上方函数返回
     */
    public static class OrderedWatermarkGenerator implements WatermarkGenerator<Event>{

        private Long maxTs = Long.MIN_VALUE;
        /**
         * 给每条数据生成水位线， 每条数据到达都会调用一次该方法
         * @param event  当前到达的数据
         * @param l  通过TimestampAssigner提取的时间
         * @param watermarkOutput
         */
        @Override
        public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
            // 原则: 以已经到达的数据中最大的时间来生成水位线
            // 因为是有序流，可以保证当前到达的数据的时间一定是最大的， 基于当前数据的时间来生成水位线即可。

            // 创建水位线对象
            //Watermark watermark = new Watermark(l);
            // 发射水位线
            //watermarkOutput.emitWatermark(watermark);
            //System.out.println("onEvent ==> 有序流每条数据生成水位线: " + watermark);

            // 记录最大时间 用于周期性生成水位线
            maxTs = l;
        }
        /**
         * 周期性生成水位线
         * 默认周期:  200ms  ,  PipelineOptions.AUTO_WATERMARK_INTERVAL = Duration.ofMillis(200)
         * 设置周期:  env.getConfig().setAutoWatermarkInterval(500);
         */

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            // 原则: 以已经到达的数据中最大的时间来生成水位线

            // 创建水位线对象
            Watermark watermark = new Watermark(maxTs);
            // 发射水位线
            watermarkOutput.emitWatermark(watermark);
            System.out.println("onPeriodicEmit ==> 有序流周期性生成水位线: " + watermark );
        }
    }
}

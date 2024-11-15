package com.atguigu.flink.datasteamapi.transformation;
/**
 *
 * 处理算子 ： process
 * 处理函数:  ProcessFunction
 * 处理函数的功能:
 *    1. 处理数据的功能  processElement()
 *    2. 拥有富函数的功能，  生命周期方法  和  按键分区状态编程
 *    3. 定时器编程
 *          TimerService:
 *               long currentProcessingTime();
 *               void registerProcessingTimeTimer(long time);
 *               void deleteProcessingTimeTimer(long time);
 *
 *               long currentWatermark();
 *               void registerEventTimeTimer(long time);
 *               void deleteEventTimeTimer(long time);
 *
 *          onTimer() :  当定时器触发后，会调用该方法
 *   4. 使用侧输出流
 *         public abstract <X> void output(OutputTag<X> outputTag, X value);
 *
 */
public class Flink06_ProcessOperator {
}

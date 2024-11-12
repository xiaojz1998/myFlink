package com.atguigu.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Flink对POJO的要求：
 *   1. 类必须是public
 *   2. 类中必须有无参数构造器
 *   3. 类中的属性必须能被访问。
 *       3.1 属性通过public修饰
 *       3.2 属性通过private修饰，提供public修饰的get、set方法
 *   4. 属性必须可以被序列化
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WordCount {
    private String word;
    private Long count;

    // 显示的时候用到tostring方法，可以考虑重写
    @Override
    public String toString() {
        return "WordCount{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
}

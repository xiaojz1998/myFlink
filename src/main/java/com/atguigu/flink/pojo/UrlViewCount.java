package com.atguigu.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UrlViewCount {
    private Long windowStart ;
    private Long windowEnd;
    private String url ;
    private Long count ;
    @Override
    public String toString() {
        return "窗口 : [ " + windowStart + " , " + windowEnd + " ) , url : " + url + " , count : " + count ;
    }
}

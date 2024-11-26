package com.atguigu.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ClickEvent {
    private String id ;
    private String provinceId ;
    private Long ts ;
}

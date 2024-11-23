package com.atguigu.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AppEvent {
    private String id;
    private String orderId;
    private String eventType ;  // pay 、 order  、 cart ....
    private Long ts ;
}

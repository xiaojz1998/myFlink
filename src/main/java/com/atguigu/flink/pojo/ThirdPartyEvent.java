package com.atguigu.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ThirdPartyEvent {
    private String id ;
    private String orderId ;
    private String eventType ;
    private String thirdPartyName ;  // wechat 、alipay
    private Long ts ;
}

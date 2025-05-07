package com.ctrip.framework.drc.service.mq;

import com.alibaba.fastjson.annotation.JSONField;

/**
 * Created by shiruixin
 * 2025/2/19 19:03
 */
public class DataChangeVo extends DataChangeMessage {
    @JSONField(ordinal = 10)
    String dc;


    public String getDc() {
        return dc;
    }

    public void setDc(String dc) {
        this.dc = dc;
    }

}

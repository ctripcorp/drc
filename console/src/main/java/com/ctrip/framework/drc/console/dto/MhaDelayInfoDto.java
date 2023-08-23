package com.ctrip.framework.drc.console.dto;

import java.io.Serializable;


public class MhaDelayInfoDto implements Serializable {
    private Long srcLastUpdateTime;
    private Long dstLastUpdateTime;
    private Long nowTime;
    private Long value;

    public MhaDelayInfoDto(Long srcLastUpdateTime, Long dstLastUpdateTime, Long nowTime, Long value) {
        this.srcLastUpdateTime = srcLastUpdateTime;
        this.dstLastUpdateTime = dstLastUpdateTime;
        this.value = value;
        this.nowTime = nowTime;
    }

    public Long getNowTime() {
        return nowTime;
    }

    public void setNowTime(Long nowTime) {
        this.nowTime = nowTime;
    }

    public void setSrcLastUpdateTime(Long srcLastUpdateTime) {
        this.srcLastUpdateTime = srcLastUpdateTime;
    }

    public void setDstLastUpdateTime(Long dstLastUpdateTime) {
        this.dstLastUpdateTime = dstLastUpdateTime;
    }

    public void setValue(Long value) {
        this.value = value;
    }

    public Long getSrcLastUpdateTime() {
        return srcLastUpdateTime;
    }

    public Long getDstLastUpdateTime() {
        return dstLastUpdateTime;
    }

    public Long getValue() {
        return value;
    }
}

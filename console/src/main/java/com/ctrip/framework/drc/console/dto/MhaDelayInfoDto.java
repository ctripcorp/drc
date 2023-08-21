package com.ctrip.framework.drc.console.dto;

import java.io.Serializable;


public class MhaDelayInfoDto implements Serializable {
    private Long srcLastUpdateTime;
    private Long dstLastUpdateTime;
    private Long value;

    public MhaDelayInfoDto(Long srcLastUpdateTime, Long dstLastUpdateTime, Long value) {
        this.srcLastUpdateTime = srcLastUpdateTime;
        this.dstLastUpdateTime = dstLastUpdateTime;
        this.value = value;
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

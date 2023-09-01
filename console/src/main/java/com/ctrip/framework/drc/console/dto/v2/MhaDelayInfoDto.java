package com.ctrip.framework.drc.console.dto.v2;

import java.io.Serializable;


public class MhaDelayInfoDto implements Serializable {

    private String srcMha;
    private String dstMha;
    private Long srcTime;
    private Long dstTime;

    @Override
    public String toString() {
        if (dstMha == null) {
            return String.format("%s: %dms", srcMha, this.getDelay());
        }
        return String.format("%s->%s: %dms", srcMha, dstMha, this.getDelay());
    }

    public String getSrcMha() {
        return srcMha;
    }

    public void setSrcMha(String srcMha) {
        this.srcMha = srcMha;
    }

    public String getDstMha() {
        return dstMha;
    }

    public void setDstMha(String dstMha) {
        this.dstMha = dstMha;
    }

    public Long getDelay() {
        if (srcTime != null && dstTime != null) {
            return srcTime - dstTime;
        } else {
            return null;
        }
    }

    public Long getSrcTime() {
        return srcTime;
    }

    public void setSrcTime(Long srcTime) {
        this.srcTime = srcTime;
    }

    public Long getDstTime() {
        return dstTime;
    }

    public void setDstTime(Long dstTime) {
        this.dstTime = dstTime;
    }
}

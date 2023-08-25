package com.ctrip.framework.drc.console.dto.v2;

import java.io.Serializable;


public class MhaDelayInfoDto implements Serializable {

    private String srcMha;
    private String dstMha;
    private Long delay;

    @Override
    public String toString() {
        return String.format("%s->%s: %dms", srcMha, dstMha, delay);
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
        return delay;
    }

    public void setDelay(Long delay) {
        this.delay = delay;
    }
}

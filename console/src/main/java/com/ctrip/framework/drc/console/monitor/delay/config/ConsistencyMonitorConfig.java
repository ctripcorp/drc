package com.ctrip.framework.drc.console.monitor.delay.config;

public class ConsistencyMonitorConfig extends DelayMonitorConfig {
    private String srcMha;
    private String dstMha;

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

    public String uniqKey() {
        return String.format("%s.%s.%s", srcMha, dstMha, getTable());
    }
}

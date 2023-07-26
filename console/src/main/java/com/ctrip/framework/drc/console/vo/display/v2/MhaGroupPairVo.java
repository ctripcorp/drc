package com.ctrip.framework.drc.console.vo.display.v2;

public class MhaGroupPairVo {
    private String srcMha;

    private String dstMha;

    private Long BuId;

    private Integer srcMhaMonitorSwitch;

    private Integer dstMhaMonitorSwitch;

    public Integer getSrcMhaMonitorSwitch() {
        return srcMhaMonitorSwitch;
    }

    public void setSrcMhaMonitorSwitch(Integer srcMhaMonitorSwitch) {
        this.srcMhaMonitorSwitch = srcMhaMonitorSwitch;
    }

    public Integer getDstMhaMonitorSwitch() {
        return dstMhaMonitorSwitch;
    }

    public void setDstMhaMonitorSwitch(Integer dstMhaMonitorSwitch) {
        this.dstMhaMonitorSwitch = dstMhaMonitorSwitch;
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

    public Long getBuId() {
        return BuId;
    }

    public void setBuId(Long buId) {
        BuId = buId;
    }

}

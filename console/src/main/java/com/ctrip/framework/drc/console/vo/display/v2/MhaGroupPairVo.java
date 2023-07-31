package com.ctrip.framework.drc.console.vo.display.v2;

public class MhaGroupPairVo {

    private String replicationId;
    private MhaVo srcMha;
    private MhaVo dstMha;

    // connection status
    private Integer status;
    private Long datachangeLasttime;

    public String getReplicationId() {
        return replicationId;
    }

    public void setReplicationId(String replicationId) {
        this.replicationId = replicationId;
    }

    public MhaVo getSrcMha() {
        return srcMha;
    }

    public void setSrcMha(MhaVo srcMha) {
        this.srcMha = srcMha;
    }

    public MhaVo getDstMha() {
        return dstMha;
    }

    public void setDstMha(MhaVo dstMha) {
        this.dstMha = dstMha;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Long getDatachangeLasttime() {
        return datachangeLasttime;
    }

    public void setDatachangeLasttime(Long datachangeLasttime) {
        this.datachangeLasttime = datachangeLasttime;
    }
}

package com.ctrip.framework.drc.console.vo.display.v2;

public class MhaReplicationVo {

    private String replicationId;
    private MhaVo srcMha;
    private MhaVo dstMha;

    /**
     * @see com.ctrip.framework.drc.console.enums.TransmissionTypeEnum
     */
    private String type;
    private Long datachangeLasttime;
    /**
     * 2: DB 粒度同步中
     * 1: MHA 同步中
     * 0: 未建立
     */
    private Integer status;

    private Long delay;

    public Long getDelay() {
        return delay;
    }

    public void setDelay(Long delay) {
        this.delay = delay;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

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


    public Long getDatachangeLasttime() {
        return datachangeLasttime;
    }

    public void setDatachangeLasttime(Long datachangeLasttime) {
        this.datachangeLasttime = datachangeLasttime;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getReplicationKey() {
        return srcMha.getName() + "->" + dstMha.getName();
    }
}

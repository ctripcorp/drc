package com.ctrip.framework.drc.console.vo.display.v2;

public class MhaGroupPairVo {

    private String replicationId;
    private String srcMhaName;
    private String dstMhaName;
    private Integer status;
    private Long srcBuId;
    private Long dstBuId;
    private Long srcDcId;
    private Long dstDcId;
    private Integer srcMhaMonitorSwitch;
    private Integer dstMhaMonitorSwitch;
    private Long datachangeLasttime;

    public String getReplicationId() {
        return replicationId;
    }

    public void setReplicationId(String replicationId) {
        this.replicationId = replicationId;
    }

    public String getSrcMhaName() {
        return srcMhaName;
    }

    public void setSrcMhaName(String srcMhaName) {
        this.srcMhaName = srcMhaName;
    }

    public String getDstMhaName() {
        return dstMhaName;
    }

    public void setDstMhaName(String dstMhaName) {
        this.dstMhaName = dstMhaName;
    }

    public Long getSrcBuId() {
        return srcBuId;
    }

    public void setSrcBuId(Long srcBuId) {
        this.srcBuId = srcBuId;
    }

    public Long getDstBuId() {
        return dstBuId;
    }

    public void setDstBuId(Long dstBuId) {
        this.dstBuId = dstBuId;
    }

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

    public Long getDatachangeLasttime() {
        return datachangeLasttime;
    }

    public void setDatachangeLasttime(Long datachangeLasttime) {
        this.datachangeLasttime = datachangeLasttime;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Long getSrcDcId() {
        return srcDcId;
    }

    public void setSrcDcId(Long srcDcId) {
        this.srcDcId = srcDcId;
    }

    public Long getDstDcId() {
        return dstDcId;
    }

    public void setDstDcId(Long dstDcId) {
        this.dstDcId = dstDcId;
    }
}

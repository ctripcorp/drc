package com.ctrip.framework.drc.console.vo.v2;

/**
 * Created by dengquanliang
 * 2023/8/3 17:56
 */
public class ResourceView {
    private Long resourceId;
    private String ip;
    private Long replicatorNum;
    private Long applierNum;
    private Integer active;

    public Long getResourceId() {
        return resourceId;
    }

    public void setResourceId(Long resourceId) {
        this.resourceId = resourceId;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Long getReplicatorNum() {
        return replicatorNum;
    }

    public void setReplicatorNum(Long replicatorNum) {
        this.replicatorNum = replicatorNum;
    }

    public Long getApplierNum() {
        return applierNum;
    }

    public void setApplierNum(Long applierNum) {
        this.applierNum = applierNum;
    }

    public Integer getActive() {
        return active;
    }

    public void setActive(Integer active) {
        this.active = active;
    }
}

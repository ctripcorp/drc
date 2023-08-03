package com.ctrip.framework.drc.console.vo.v2;

/**
 * Created by dengquanliang
 * 2023/8/3 17:56
 */
public class ResourceView {
    private Long resourceId;
    private String ip;
    private Integer replicatorNum;
    private Integer applierNum;
    private Integer active;
    private Integer deleted;

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

    public Integer getReplicatorNum() {
        return replicatorNum;
    }

    public void setReplicatorNum(Integer replicatorNum) {
        this.replicatorNum = replicatorNum;
    }

    public Integer getApplierNum() {
        return applierNum;
    }

    public void setApplierNum(Integer applierNum) {
        this.applierNum = applierNum;
    }

    public Integer getActive() {
        return active;
    }

    public void setActive(Integer active) {
        this.active = active;
    }

    public Integer getDeleted() {
        return deleted;
    }

    public void setDeleted(Integer deleted) {
        this.deleted = deleted;
    }
}

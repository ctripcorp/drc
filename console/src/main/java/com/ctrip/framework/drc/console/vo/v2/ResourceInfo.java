package com.ctrip.framework.drc.console.vo.v2;

/**
 * Created by dengquanliang
 * 2023/8/4 14:51
 */
public class ResourceInfo {
    private String ip;
    private String az;
    private long replicatorNum;
    private long applierNum;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getAz() {
        return az;
    }

    public void setAz(String az) {
        this.az = az;
    }

    public long getReplicatorNum() {
        return replicatorNum;
    }

    public void setReplicatorNum(long replicatorNum) {
        this.replicatorNum = replicatorNum;
    }

    public long getApplierNum() {
        return applierNum;
    }

    public void setApplierNum(long applierNum) {
        this.applierNum = applierNum;
    }
}

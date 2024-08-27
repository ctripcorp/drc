package com.ctrip.framework.drc.console.dto.v3;

public class ReplicatorInfoDto {
    private String ip;
    // same for same mha
    private String gtidInit;
    private long replicatorId;
    private String tag;
    private String az;
    private boolean master;

    public ReplicatorInfoDto(String ip, String gtidInit) {
        this.ip = ip;
        this.gtidInit = gtidInit;
    }
    
    public ReplicatorInfoDto(long replicatorId,String gtidInit,boolean master,String ip, String tag, String az) {
        this.replicatorId = replicatorId;
        this.gtidInit = gtidInit;
        this.master = master;
        this.ip = ip;
        this.tag = tag;
        this.az = az;
    }
    
    public long getReplicatorId() {
        return replicatorId;
    }
    
    public void setReplicatorId(long replicatorId) {
        this.replicatorId = replicatorId;
    }
    
    
    public String getAz() {
        return az;
    }

    public void setAz(String az) {
        this.az = az;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public boolean getMaster() {
        return master;
    }

    public void setMaster(boolean master) {
        this.master = master;
    }

    public String getGtidInit() {
        return gtidInit;
    }

    public void setGtidInit(String gtidInit) {
        this.gtidInit = gtidInit;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }
}

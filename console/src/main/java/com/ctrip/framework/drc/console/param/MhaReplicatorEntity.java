package com.ctrip.framework.drc.console.param;

/**
 * Created by dengquanliang
 * 2024/10/29 21:10
 */
public class MhaReplicatorEntity {
    private String mhaName;
    private String replicatorIp;

    public MhaReplicatorEntity(String mhaName, String replicatorIp) {
        this.mhaName = mhaName;
        this.replicatorIp = replicatorIp;
    }

    public MhaReplicatorEntity() {
    }

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public String getReplicatorIp() {
        return replicatorIp;
    }

    public void setReplicatorIp(String replicatorIp) {
        this.replicatorIp = replicatorIp;
    }

    @Override
    public String toString() {
        return "MhaReplicatorEntity{" +
                "mhaName='" + mhaName + '\'' +
                ", replicatorIp='" + replicatorIp + '\'' +
                '}';
    }
}

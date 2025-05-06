package com.ctrip.framework.drc.console.param;

import java.util.Map;

/**
 * Created by dengquanliang
 * 2024/10/29 21:10
 */
public class MhaReplicatorEntity {
    private String mhaName;
    private String replicatorIp;
    Map<String, String> mhaName2ReplicatorIps;


    public MhaReplicatorEntity(Map<String, String> mhaName2ReplicatorIps) {
        this.mhaName2ReplicatorIps = mhaName2ReplicatorIps;
    }

    public MhaReplicatorEntity(String mhaName, String replicatorIp) {
        this.mhaName = mhaName;
        this.replicatorIp = replicatorIp;
    }

    public MhaReplicatorEntity() {
    }

    public Map<String, String> getMhaName2ReplicatorIps() {
        return mhaName2ReplicatorIps;
    }

    public void setMhaName2ReplicatorIps(Map<String, String> mhaName2ReplicatorIps) {
        this.mhaName2ReplicatorIps = mhaName2ReplicatorIps;
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

package com.ctrip.framework.drc.console.dto.v3;

public class ReplicatorInfoDto {
    private String ip;

    // same for same mha
    private String gtidInit;

    public ReplicatorInfoDto(String ip, String gtidInit) {
        this.ip = ip;
        this.gtidInit = gtidInit;
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

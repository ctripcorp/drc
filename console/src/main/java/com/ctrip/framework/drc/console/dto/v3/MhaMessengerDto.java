package com.ctrip.framework.drc.console.dto.v3;

import java.util.Collections;
import java.util.List;

public class MhaMessengerDto {
    private List<String> ips;
    private String mqType;
    private String gtidInit;

    public MhaMessengerDto() {
        this.ips = Collections.emptyList();
    }

    public MhaMessengerDto(List<String> ips, String mqType, String gtidInit) {
        this.ips = ips;
        this.mqType = mqType;
        this.gtidInit = gtidInit;
    }

    public String getMqType() {
        return mqType;
    }

    public void setMqType(String mqType) {
        this.mqType = mqType;
    }

    public List<String> getIps() {
        return ips;
    }

    public void setIps(List<String> ips) {
        this.ips = ips;
    }

    public String getGtidInit() {
        return gtidInit;
    }

    public void setGtidInit(String gtidInit) {
        this.gtidInit = gtidInit;
    }
}

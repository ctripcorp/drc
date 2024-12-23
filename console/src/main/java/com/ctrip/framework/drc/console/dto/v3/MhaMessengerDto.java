package com.ctrip.framework.drc.console.dto.v3;

import java.util.Collections;
import java.util.List;

public class MhaMessengerDto {
    private List<String> ips;
    private String gtidInit;

    public MhaMessengerDto() {
        this.ips = Collections.emptyList();
    }

    public MhaMessengerDto(List<String> ips, String gtidInit) {
        this.ips = ips;
        this.gtidInit = gtidInit;
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

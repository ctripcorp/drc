package com.ctrip.framework.drc.console.dto.v3;

import java.util.List;

public class MhaApplierDto {
    private List<String> ips;
    private String gtidInit;

    public MhaApplierDto() {
    }

    public MhaApplierDto(List<String> ips, String gtidInit) {
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

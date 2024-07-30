package com.ctrip.framework.drc.console.param.v2.resource;

import java.util.List;

/**
 * Created by dengquanliang
 * 2024/6/6 16:22
 */
public class ApplierMigrateParam {
    private String oldIp;
    private String newIp;
    private List<ApplierResourceDto> applierResourceDtos;

    public String getOldIp() {
        return oldIp;
    }

    public void setOldIp(String oldIp) {
        this.oldIp = oldIp;
    }

    public String getNewIp() {
        return newIp;
    }

    public void setNewIp(String newIp) {
        this.newIp = newIp;
    }

    public List<ApplierResourceDto> getApplierResourceDtos() {
        return applierResourceDtos;
    }

    public void setApplierResourceDtos(List<ApplierResourceDto> applierResourceDtos) {
        this.applierResourceDtos = applierResourceDtos;
    }

    @Override
    public String toString() {
        return "ApplierMigrateParam{" +
                "oldIp='" + oldIp + '\'' +
                ", newIp='" + newIp + '\'' +
                ", applierResourceDtos=" + applierResourceDtos +
                '}';
    }
}

package com.ctrip.framework.drc.console.param.v2.resource;

/**
 * Created by dengquanliang
 * 2024/3/1 16:52
 */
public class ResourceMigrateDto {
    private String newIp;
    private String oldIp;

    public String getNewIp() {
        return newIp;
    }

    public void setNewIp(String newIp) {
        this.newIp = newIp;
    }

    public String getOldIp() {
        return oldIp;
    }

    public void setOldIp(String oldIp) {
        this.oldIp = oldIp;
    }

    @Override
    public String toString() {
        return "ResourceMigrateDto{" +
                "newIp='" + newIp + '\'' +
                ", oldIp='" + oldIp + '\'' +
                '}';
    }
}

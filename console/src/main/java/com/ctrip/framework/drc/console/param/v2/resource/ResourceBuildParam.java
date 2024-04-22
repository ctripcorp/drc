package com.ctrip.framework.drc.console.param.v2.resource;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/8/3 16:12
 */
public class ResourceBuildParam {
    private String ip;
    private String type;
    private String dcName;
    private String tag;
    private String az;
    private List<String> ips;

    public List<String> getIps() {
        return ips;
    }

    public void setIps(List<String> ips) {
        this.ips = ips;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDcName() {
        return dcName;
    }

    public void setDcName(String dcName) {
        this.dcName = dcName;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getAz() {
        return az;
    }

    public void setAz(String az) {
        this.az = az;
    }

    @Override
    public String toString() {
        return "ResourceBuildParam{" +
                "ip='" + ip + '\'' +
                ", type='" + type + '\'' +
                ", dcName='" + dcName + '\'' +
                ", tag='" + tag + '\'' +
                ", az='" + az + '\'' +
                ", ips=" + ips +
                '}';
    }
}

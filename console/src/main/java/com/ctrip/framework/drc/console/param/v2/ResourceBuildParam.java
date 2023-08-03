package com.ctrip.framework.drc.console.param.v2;

/**
 * Created by dengquanliang
 * 2023/8/3 16:12
 */
public class ResourceBuildParam {
    private String ip;
    private String type;
    private Long dcId;
    private String tag;
    private String azId;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getType() {
        return type;
    }

    public void String(String type) {
        this.type = type;
    }

    public Long getDcId() {
        return dcId;
    }

    public void setDcId(Long dcId) {
        this.dcId = dcId;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getAzId() {
        return azId;
    }

    public void setAzId(String azId) {
        this.azId = azId;
    }
}

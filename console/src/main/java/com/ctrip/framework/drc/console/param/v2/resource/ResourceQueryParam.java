package com.ctrip.framework.drc.console.param.v2.resource;

import com.ctrip.framework.drc.core.http.PageReq;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/8/3 17:35
 */
public class ResourceQueryParam {
    private String ip;
    private Integer type;
    private String tag;
    private Integer active;
    private String region;
    private List<Long> dcIds;
    private PageReq pageReq;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public Integer getActive() {
        return active;
    }

    public void setActive(Integer active) {
        this.active = active;
    }

    public PageReq getPageReq() {
        return pageReq;
    }

    public void setPageReq(PageReq pageReq) {
        this.pageReq = pageReq;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public List<Long> getDcIds() {
        return dcIds;
    }

    public void setDcIds(List<Long> dcIds) {
        this.dcIds = dcIds;
    }

    @Override
    public String toString() {
        return "ResourceQueryParam{" +
                "ip='" + ip + '\'' +
                ", type=" + type +
                ", tag='" + tag + '\'' +
                ", active=" + active +
                ", region='" + region + '\'' +
                ", dcIds=" + dcIds +
                ", pageReq=" + pageReq +
                '}';
    }
}

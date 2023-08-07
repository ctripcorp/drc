package com.ctrip.framework.drc.console.param.v2.resource;

import com.ctrip.framework.drc.core.http.PageReq;

/**
 * Created by dengquanliang
 * 2023/8/3 17:35
 */
public class ResourceQueryParam {
    private String ip;
    private Integer type;
    private Long dcId;
    private String tag;
    private Integer active;
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
}

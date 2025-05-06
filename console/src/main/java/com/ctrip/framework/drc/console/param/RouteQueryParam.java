package com.ctrip.framework.drc.console.param;

import java.util.List;

/**
 * Created by dengquanliang
 * 2025/4/3 14:58
 */
public class RouteQueryParam {
    private long buId;
    private String srcDcName;
    private String dstDcName;
    private String tag;
    private Integer globalActive;
    private List<Long> srcDcIds;
    private List<Long> dstDcIds;

    public List<Long> getSrcDcIds() {
        return srcDcIds;
    }

    public void setSrcDcIds(List<Long> srcDcIds) {
        this.srcDcIds = srcDcIds;
    }

    public List<Long> getDstDcIds() {
        return dstDcIds;
    }

    public void setDstDcIds(List<Long> dstDcIds) {
        this.dstDcIds = dstDcIds;
    }

    public Integer getGlobalActive() {
        return globalActive;
    }

    public void setGlobalActive(Integer globalActive) {
        this.globalActive = globalActive;
    }

    public long getBuId() {
        return buId;
    }

    public void setBuId(long buId) {
        this.buId = buId;
    }

    public String getSrcDcName() {
        return srcDcName;
    }

    public void setSrcDcName(String srcDcName) {
        this.srcDcName = srcDcName;
    }

    public String getDstDcName() {
        return dstDcName;
    }

    public void setDstDcName(String dstDcName) {
        this.dstDcName = dstDcName;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }
}

package com.ctrip.framework.drc.console.pojo.domain;

public class DcDo {
    private Long dcId;

    private String dcName;

    private Long regionId;

    private String regionName;

    public Long getDcId() {
        return dcId;
    }

    public void setDcId(Long dcId) {
        this.dcId = dcId;
    }

    public String getDcName() {
        return dcName;
    }

    public void setDcName(String dcName) {
        this.dcName = dcName;
    }

    public Long getRegionId() {
        return regionId;
    }

    public void setRegionId(Long regionId) {
        this.regionId = regionId;
    }

    public String getRegionName() {
        return regionName;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }
}

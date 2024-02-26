package com.ctrip.framework.drc.console.dto.v3;

import org.apache.commons.lang3.StringUtils;

public class MhaDbReplicationCreateDto {
    private String srcRegionName;
    private String dstRegionName;
    private String dbName;
    private String buName;
    private String tag;


    public void validAndTrimForCreateReq() {
        if (StringUtils.isBlank(srcRegionName)) {
            throw new IllegalArgumentException("srcRegionName should not be blank!");
        }
        if (StringUtils.isBlank(dstRegionName)) {
            throw new IllegalArgumentException("dstRegionName should not be blank!");
        }
        if (StringUtils.isBlank(dbName)) {
            throw new IllegalArgumentException("dbName should not be blank!");
        }
        srcRegionName = srcRegionName.trim();
        dstRegionName = dstRegionName.trim();
        dbName = dbName.trim();
    }

    public String getSrcRegionName() {
        return srcRegionName;
    }

    public void setSrcRegionName(String srcRegionName) {
        this.srcRegionName = srcRegionName;
    }

    public String getDstRegionName() {
        return dstRegionName;
    }

    public void setDstRegionName(String dstRegionName) {
        this.dstRegionName = dstRegionName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getBuName() {
        return buName;
    }

    public void setBuName(String buName) {
        this.buName = buName;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }
}
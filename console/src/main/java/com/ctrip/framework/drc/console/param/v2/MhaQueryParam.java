package com.ctrip.framework.drc.console.param.v2;

import com.ctrip.framework.drc.core.http.PageReq;

import java.util.List;

/**
 * Created by dengquanliang
 * 2024/3/25 14:10
 */
public class MhaQueryParam {
    private String mhaName;
    private String dbName;
    private String regionName;
    private List<Long> dcIds;
    private List<Long> mhaIds;
    private PageReq pageReq;

    public List<Long> getDcIds() {
        return dcIds;
    }

    public void setDcIds(List<Long> dcIds) {
        this.dcIds = dcIds;
    }

    public List<Long> getMhaIds() {
        return mhaIds;
    }

    public void setMhaIds(List<Long> mhaIds) {
        this.mhaIds = mhaIds;
    }

    public PageReq getPageReq() {
        return pageReq;
    }

    public void setPageReq(PageReq pageReq) {
        this.pageReq = pageReq;
    }

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getRegionName() {
        return regionName;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }
}

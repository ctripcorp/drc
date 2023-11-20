package com.ctrip.framework.drc.console.param.log;

import com.ctrip.framework.drc.core.http.PageReq;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/9/26 14:56
 */
public class ConflictTrxLogQueryParam {

    private String srcMhaName;
    private String dstMhaName;
    private String db;
    private List<String> dbsWithPermission; 
    private boolean admin;
    private String gtId;
    private Long beginHandleTime;
    private Long endHandleTime;
    private Integer trxResult;
    private PageReq pageReq;

    public String getSrcMhaName() {
        return srcMhaName;
    }

    public void setSrcMhaName(String srcMhaName) {
        this.srcMhaName = srcMhaName;
    }

    public String getDstMhaName() {
        return dstMhaName;
    }

    public void setDstMhaName(String dstMhaName) {
        this.dstMhaName = dstMhaName;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public List<String> getDbsWithPermission() {
        return dbsWithPermission;
    }

    public void setDbsWithPermission(List<String> dbsWithPermission) {
        this.dbsWithPermission = dbsWithPermission;
    }

    public boolean isAdmin() {
        return admin;
    }

    public void setAdmin(boolean admin) {
        this.admin = admin;
    }

    public String getGtId() {
        return gtId;
    }

    public void setGtId(String gtId) {
        this.gtId = gtId;
    }

    public Long getBeginHandleTime() {
        return beginHandleTime;
    }

    public void setBeginHandleTime(Long beginHandleTime) {
        this.beginHandleTime = beginHandleTime;
    }

    public Long getEndHandleTime() {
        return endHandleTime;
    }

    public void setEndHandleTime(Long endHandleTime) {
        this.endHandleTime = endHandleTime;
    }

    public PageReq getPageReq() {
        return pageReq;
    }

    public void setPageReq(PageReq pageReq) {
        this.pageReq = pageReq;
    }

    public Integer getTrxResult() {
        return trxResult;
    }

    public void setTrxResult(Integer trxResult) {
        this.trxResult = trxResult;
    }
}

package com.ctrip.framework.drc.console.param.log;

import com.ctrip.framework.drc.core.http.PageReq;

/**
 * Created by dengquanliang
 * 2023/9/26 15:12
 */
public class ConflictRowsLogQueryParam {
    private Long conflictTrxLogId;
    private String gtid;
    private String dbName;
    private String tableName;
    private Long beginHandleTime;
    private Long endHandleTime;
    private Integer rowResult;
    private PageReq pageReq;

    public String getGtid() {
        return gtid;
    }

    public void setGtid(String gtid) {
        this.gtid = gtid;
    }

    public Long getConflictTrxLogId() {
        return conflictTrxLogId;
    }

    public void setConflictTrxLogId(Long conflictTrxLogId) {
        this.conflictTrxLogId = conflictTrxLogId;
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

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Integer getRowResult() {
        return rowResult;
    }

    public void setRowResult(Integer rowResult) {
        this.rowResult = rowResult;
    }

    public PageReq getPageReq() {
        return pageReq;
    }

    public void setPageReq(PageReq pageReq) {
        this.pageReq = pageReq;
    }
}

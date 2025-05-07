package com.ctrip.framework.drc.console.param.log;

import com.ctrip.framework.drc.core.http.PageReq;
import org.springframework.beans.BeanUtils;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/9/26 15:12
 */
public class ConflictRowsLogQueryParam {
    private Long conflictTrxLogId;
    private String gtid;
    private List<String> dbsWithPermission;
    private boolean admin;
    private String dbName;
    private String tableName;
    private Long beginHandleTime;
    private Long endHandleTime;
    private String createBeginTime;
    private String createEndTime;
    private String srcRegion;
    private String dstRegion;
    private Integer rowResult;
    private Integer brief;
    private boolean likeSearch;
    private PageReq pageReq;

    public String getCreateBeginTime() {
        return createBeginTime;
    }

    public void setCreateBeginTime(String createBeginTime) {
        this.createBeginTime = createBeginTime;
    }

    public String getCreateEndTime() {
        return createEndTime;
    }

    public void setCreateEndTime(String createEndTime) {
        this.createEndTime = createEndTime;
    }

    public boolean isLikeSearch() {
        return likeSearch;
    }

    public void setLikeSearch(boolean likeSearch) {
        this.likeSearch = likeSearch;
    }

    public String getSrcRegion() {
        return srcRegion;
    }

    public void setSrcRegion(String srcRegion) {
        this.srcRegion = srcRegion;
    }

    public String getDstRegion() {
        return dstRegion;
    }

    public void setDstRegion(String dstRegion) {
        this.dstRegion = dstRegion;
    }

    public String getGtid() {
        return gtid;
    }

    public void setGtid(String gtid) {
        this.gtid = gtid;
    }

    public boolean isAdmin() {
        return admin;
    }

    public void setAdmin(boolean admin) {
        this.admin = admin;
    }

    public List<String> getDbsWithPermission() {
        return dbsWithPermission;
    }

    public void setDbsWithPermission(List<String> dbsWithPermission) {
        this.dbsWithPermission = dbsWithPermission;
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

    public Integer getBrief() {
        return brief;
    }

    public void setBrief(Integer brief) {
        this.brief = brief;
    }

    @Override
    public String toString() {
        return "ConflictRowsLogQueryParam{" +
                "conflictTrxLogId=" + conflictTrxLogId +
                ", gtid='" + gtid + '\'' +
                ", admin=" + admin +
                ", dbName='" + dbName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", beginHandleTime=" + beginHandleTime +
                ", endHandleTime=" + endHandleTime +
                ", srcRegion='" + srcRegion + '\'' +
                ", dstRegion='" + dstRegion + '\'' +
                ", rowResult=" + rowResult +
                ", brief=" + brief +
                ", likeSearch=" + likeSearch +
                ", pageReq=" + pageReq +
                '}';
    }
}

package com.ctrip.framework.drc.console.param.v2.application;

import com.ctrip.framework.drc.core.http.PageReq;

import java.util.List;

/**
 * Created by dengquanliang
 * 2024/1/31 19:35
 */
public class ApplicationFormQueryParam {
    private String dbName;
    private String tableName;
    private String srcRegion;
    private String dstRegion;
    private Integer replicationType;
    private String filterType;
    private Integer approvalResult;
    private List<Long> applicationFormIds;
    private PageReq pageReq;

    public List<Long> getApplicationFormIds() {
        return applicationFormIds;
    }

    public void setApplicationFormIds(List<Long> applicationFormIds) {
        this.applicationFormIds = applicationFormIds;
    }

    public Integer getApprovalResult() {
        return approvalResult;
    }

    public void setApprovalResult(Integer approvalResult) {
        this.approvalResult = approvalResult;
    }

    public PageReq getPageReq() {
        return pageReq;
    }

    public void setPageReq(PageReq pageReq) {
        this.pageReq = pageReq;
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

    public Integer getReplicationType() {
        return replicationType;
    }

    public void setReplicationType(Integer replicationType) {
        this.replicationType = replicationType;
    }

    public String getFilterType() {
        return filterType;
    }

    public void setFilterType(String filterType) {
        this.filterType = filterType;
    }
}

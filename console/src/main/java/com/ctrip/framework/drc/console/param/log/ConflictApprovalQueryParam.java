package com.ctrip.framework.drc.console.param.log;

import com.ctrip.framework.drc.core.http.PageReq;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/10/31 11:15
 */
public class ConflictApprovalQueryParam {
    private String dbName;
    private String tableName;
    private String applicant;
    private Integer approvalResult;
    private List<Long> batchIds;
    private List<String> dbsWithPermission;
    private boolean admin;
    private PageReq pageReq;

    public List<Long> getBatchIds() {
        return batchIds;
    }

    public void setBatchIds(List<Long> batchIds) {
        this.batchIds = batchIds;
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

    public String getApplicant() {
        return applicant;
    }

    public void setApplicant(String applicant) {
        this.applicant = applicant;
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
}
